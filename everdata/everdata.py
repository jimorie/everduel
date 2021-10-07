import aiosqlite
import json
import pathlib
import sqlite3
import typing as t
import weakref


ID_KEY = "_id"
TYPE_KEY = "_type"

registered_types = {}


def register_type(thing_type: str) -> t.Callable:
    def decorator(thing_cls: type) -> type:
        if thing_type in registered_types:
            raise RuntimeError(f"ambiguous thing type: {thing_type}")
        setattr(thing_cls, TYPE_KEY, thing_type)
        registered_types[thing_type] = thing_cls
        return thing_cls

    return decorator


class ThingEncoder(json.JSONEncoder):
    def default(self, thing):
        if isinstance(thing, BaseThing):
            if thing._id:
                return {ID_KEY: thing._id}
            if ID_KEY in thing._data:
                raise ValueError(f"refuse to save thing with {ID_KEY} in its data")
            if TYPE_KEY not in thing._data:
                thing._data[TYPE_KEY] = thing._type
            return thing._data
        return thing


class Database:
    def __init__(self, db: aiosqlite.Connection):
        self.db = db

    @classmethod
    async def connect(cls, dbpath: t.Union[str, pathlib.Path]):
        return await cls(await aiosqlite.connect(dbpath))

    async def commit(self):
        return await self.db.commit()

    async def close(self):
        return await self.db.close()

    async def execute(self, sql: str, **params) -> aiosqlite.cursor.Cursor:
        return await self.db.execute(sql, params)

    async def fetchall(self, sql: str, **params) -> t.AsyncIterator[sqlite3.Row]:
        cursor = await self.execute(sql, **params)
        async for row in cursor:
            yield row
        await cursor.close()

    async def fetchone(self, sql: str, **params) -> sqlite3.Row:
        cursor = await self.execute(sql, **params)
        row = await cursor.fetchone()
        await cursor.close()
        return row


class ThingDatabase(Database):
    version = 1

    def __init__(self, db: aiosqlite.Connection, tablename: str):
        super(ThingDatabase, self).__init__(db)
        self._loadsql = f"SELECT version, type, data FROM {tablename} WHERE id = :id"
        self._deletesql = f"DELETE FROM {tablename} WHERE id = :id"
        self._createsql = (
            f"INSERT INTO {tablename} (version, type, data) "
            f"VALUES (:version, :type, :data)"
        )
        self._updatesql = f"UPDATE {tablename} SET data = :data WHERE id = :id"
        self._cache = weakref.WeakValueDictionary()
        self._encoder = ThingEncoder()

    @classmethod
    async def connect(cls, dbpath: t.Union[str, pathlib.Path], tablename: str):
        db = cls(await aiosqlite.connect(dbpath), tablename)
        await db.initdb(tablename)
        return db

    async def initdb(self, tablename: str):
        await self.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tablename} (
                id INTEGER PRIMARY KEY,
                version INTEGER NOT NULL,
                type TEXT NOT NULL,
                data BLOB NOT NULL
            );
            """
        )

    async def create(self, thing_type: str, data: dict) -> int:
        cursor = await self.execute(
            self._createsql,
            version=self.version,
            type=thing_type,
            data=self._encoder.encode(data),
        )
        await self.commit()
        return cursor.lastrowid

    async def update(self, thing_id: int, data: dict) -> aiosqlite.cursor.Cursor:
        cursor = await self.execute(
            self._updatesql, id=thing_id, data=self._encoder.encode(data)
        )
        await self.commit()
        await cursor.close()
        return cursor

    async def delete(self, thing_id: int) -> aiosqlite.cursor.Cursor:
        cursor = self.execute(self._deletesql, id=thing_id)
        await self.commit()
        await cursor.close()
        return cursor

    async def load(self, thing_id: int, visited=None) -> "BaseThing":
        if thing_id in self._cache:
            return self._cache[thing_id]
        version, thing_type, data = await self.fetchone(self._loadsql, id=thing_id)
        data = json.loads(data)
        if version != self.version:
            # TODO: Migration
            pass
        thing = await self.from_data(data, thing_type, visited)
        thing._id = thing_id
        self._cache[thing_id] = thing
        return thing

    async def from_data(self, data: dict, thing_type=None, visited=None):
        if thing_type is None:
            thing_type = data[TYPE_KEY]
        thing_cls = registered_types.get(thing_type)
        thing = thing_cls(self, data=data)
        await thing.load_props(visited)
        return thing

    async def save(self, thing: "BaseThing") -> int:
        thing_id = thing._id
        if thing_id:
            await self.update(thing_id, thing._data)
        else:
            thing_id = await self.create(thing._type, thing._data)
            self._cache[thing_id] = thing
        return thing_id


class BaseThing:
    _type = "X"

    def __init__(
        self,
        db: ThingDatabase,
        data: t.Optional[dict] = None,
        thing_id: t.Optional[int] = None,
    ):
        self._db = db
        self._data = data or {}
        self._id = thing_id

    async def save(self) -> int:
        self._id = await self._db.save(self)
        return self._id

    async def delete(self):
        if self._id:
            await self._db.delete(self._id)
            self._id = None

    def clear(self):
        self._data.clear()

    async def load_props(self, visited=None):
        if visited is None:
            visited = set()
        elif self in visited:
            return
        visited.add(self)
        for k, v in self._data.items():
            if type(v) is dict:
                if ID_KEY in v:
                    self._data[k] = await self._db.load(v[ID_KEY], visited=visited)
                elif TYPE_KEY in v:
                    self._data[k] = await self._db.from_data(v, visited=visited)
            elif type(v) is list:
                for i in range(len(v)):
                    if type(v[i]) is dict:
                        if ID_KEY in v[i]:
                            v[i] = await self._db.load(v[i][ID_KEY], visited=visited)
                        elif TYPE_KEY in v[i]:
                            v[i] = await self._db.from_data(v[i], visited=visited)


class Property:
    def __init__(
        self,
        default: t.Any = None,
        typecheck: type = None,
        volatile: bool = False,
    ):
        self.default = default
        self._type = typecheck
        self.volatile = volatile  # TODO: Implement :)

    def __set_name__(self, owner: type, name: str):
        self.name = name

    def __get__(self, instance: BaseThing, owner: type = None) -> t.Any:
        if self.name not in instance._data:
            if self.default is None:
                return None
            if isinstance(self.default, type):
                instance._data[self.name] = self.default()
            else:
                instance._data[self.name] = self.default
        return instance._data[self.name]

    def __set__(self, instance: BaseThing, value: t.Any):
        instance._data[self.name] = self.typecheck(value)

    def __delete__(self, instance: BaseThing):
        try:
            del instance._data[self.name]
        except KeyError:
            pass

    def typecheck(self, value: t.Any) -> t.Any:
        if self._type is None:
            return value
        if type(self._type) is str:
            self._type = registered_types[self._type]
        if isinstance(value, self._type):
            return value
        raise ValueError(f"{self.name} must be of type {self._type}")


prop = Property


@register_type("T")
class Thing(BaseThing):
    name = prop()


@register_type("W")
class Weapon(Thing):
    damage = prop()


@register_type("P")
class Player(Thing):
    race = prop()
    weapon = prop(typecheck=Weapon)
    target = prop(typecheck="P")
    buddy = prop(typecheck="P")
    inventory = prop(default=list, typecheck=list)
