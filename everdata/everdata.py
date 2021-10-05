import json
import pathlib
import typing as t
import weakref

import aiosqlite
import sqlite3


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
            data=json.dumps(data),
        )
        await self.commit()
        return cursor.lastrowid

    async def update(self, thing_id: int, data: dict) -> aiosqlite.cursor.Cursor:
        cursor = await self.execute(self._updatesql, id=thing_id, data=json.dumps(data))
        await self.commit()
        await cursor.close()
        return cursor

    async def delete(self, thing_id: int) -> aiosqlite.cursor.Cursor:
        cursor = self.execute(self._deletesql, id=thing_id)
        await self.commit()
        await cursor.close()
        return cursor

    async def load(self, thing_id: int) -> "BaseThing":
        if thing_id in self._cache:
            return self._cache[thing_id]
        version, thing_type, data = await self.fetchone(self._loadsql, id=thing_id)
        thing_cls = registered_types.get(thing_type)
        data = json.loads(data)
        if version != self.version:
            # TODO: Migration
            pass
        thing = thing_cls(self, data=data, thing_id=thing_id)
        await thing.load_root_props()
        self._cache[thing_id] = thing
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
    _root = False
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
        self._cache = {}
        if not self._id and TYPE_KEY not in self._data:
            self._data[TYPE_KEY] = self._type

    async def load_root_props(self, visited=None):
        if visited is None:
            visited = set()
        elif self._id in visited:
            return
        visited.add(self._id)
        for k, v in self._data.items():
            if type(v) is dict:
                if ID_KEY in v:
                    self._cache[k] = await self._db.load(v[ID_KEY])
                else:
                    self._cache[k].load_props()

    async def save(self) -> int:
        self._id = await self._db.save(self)
        return self._id

    async def delete(self) -> t.Optional[aiosqlite.cursor.Cursor]:
        self.clear()
        if self._id:
            return await self._db.delete(self._id)
        return None

    def clear(self):
        self._data = {}


class Property:
    def __init__(self, default: t.Any = None, typecheck: type = None):
        self.default = default
        self._type = typecheck

    def __set_name__(self, owner: type, name: str):
        self.name = name

    def __get__(self, instance: BaseThing, owner: type = None) -> t.Any:
        return instance._data.get(self.name, self.default)

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


class CachedProperty(Property):
    def __init__(self, volatile: bool = False, default: t.Any = None, **kwargs):
        self.volatile = volatile
        super(CachedProperty, self).__init__(**kwargs)

    def __get__(self, instance: BaseThing, owner: type = None) -> t.Any:
        if self.name not in instance._cache and self.name in instance._data:
            instance._cache[self.name] = self.value(instance)
        return instance._cache.get(self.name)

    def __set__(self, instance: BaseThing, value: t.Optional[BaseThing]):
        if value is None:
            self.__delete(instance)
            return
        if not isinstance(value, BaseThing):
            raise ValueError(f"{self.name} must be of type BaseThing")
        instance._cache[self.name] = self.typecheck(value)
        if not self.volatile:
            if self._id:
                instance._data[self.name] = {ID_KEY: value._id}
            if self._root:
                raise RuntimeError("missing id attribute on root object")
            instance._data[self.name] = value._data

    def __delete__(self, instance: BaseThing):
        super(CachedProperty, self).__delete__(instance)
        try:
            del instance._cache[self.name]
        except KeyError:
            pass

    def value(self, instance: BaseThing) -> t.Any:
        raise NotImplementedError()


class ThingProperty(CachedProperty):
    def value(self, instance: BaseThing) -> t.Any:
        data = instance._data[self.name]
        thing_cls = registered_types[data[TYPE_KEY]]
        return thing_cls(instance._db, data)


thing = ThingProperty


class ThingList(list):
    def __init__(self, db: ThingDatabase, data: list):
        self._data = data
        super(list, self).__init__(
            registered_types[item[TYPE_KEY]](item, db) for item in data
        )

    def append(self, x):
        super(ThingList, self).append(x)
        self._data.append(x._data)

    def extend(self, iterable):
        for x in iterable:
            self.append(x)

    def insert(self, i, x):
        raise NotImplementedError()

    def remove(self, x):
        super(ThingList, self).remove(x)
        for i, d in enumerate(self._data):
            if d is x._data:
                self._data.pop(i)
                return
        raise RuntimeError("ThingList out of sync")

    def pop(self, i):
        raise NotImplementedError()

    def clear(self):
        super(ThingList, self).clear()
        self._data.clear()

    def __add__(self, x):
        raise NotImplementedError()

    def __setitem__(self, i, x):
        raise NotImplementedError()


class ThingListProperty(CachedProperty):
    def __init__(self, default: t.Any = None):
        if default is None:
            default = []
        super(ThingListProperty, self).__init__(default=default)

    def value(self, instance: BaseThing) -> t.Any:
        return ThingList(instance._db, instance._data.get(self.name, self.default))


thinglist = ThingListProperty


@register_type("T")
class Thing(BaseThing):
    name = prop()


@register_type("W")
class Weapon(Thing):
    damage = prop()


@register_type("P")
class Player(Thing):
    _root = True
    race = thing()
    weapon = thing(typecheck=Weapon)
    target = thing(typecheck="P", volatile=True)
    buddy = thing(typecheck="P")
    inventory = thinglist()
