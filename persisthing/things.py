from __future__ import annotations
import operator
import typing as t

import persisthing as pt


DEFAULT_NONE = object()

known_things = {}


def thing(thing_type: str) -> t.Callable:
    def decorator(thing_cls: type) -> type:
        if thing_type in known_things:
            raise RuntimeError(f"ambiguous thing type: {thing_type}")
        known_things[thing_type] = thing_cls
        thing_cls._type = thing_type
        return thing_cls

    return decorator


def get_thing_type(name):
    return known_things.get(name)


class BaseThing:
    _type = None
    _version = 1

    def __init__(
        self,
        _db: pt.ThingsDB = None,
        _id: t.Optional[int] = None,
        _data: t.Optional[dict] = None,
        **kwargs,
    ):
        self._db = _db
        self._id = _id
        self._data = _data or {}
        self._volatile = {}
        for k, v in kwargs.items():
            setattr(self, k, v)

    def set_db(self, db: t.Optional[pt.ThingsDB]):
        if db is None or self._db is None:
            self._db = db
        elif self._db != db:
            raise RuntimeError("cannot change database")

    async def save(self, db: pt.ThingsDB = None) -> int:
        if db:
            self.set_db(db)
        if not self._db:
            raise RuntimeError("cannot save thing without database")
        await self._db.save(self)
        return self._id

    async def delete(self):
        if self._id and self._db:
            await self._db.delete(self)

    async def clear(self):
        await self._data.clear()

    async def load_props(self, visited=None):
        if visited is None:
            visited = set()
        elif self in visited:
            return
        visited.add(self)
        for key, value in self._data.items():
            if isinstance(value, dict):
                if pt.ID_KEY in value:
                    self._data[key] = await self._db.load(
                        value[pt.ID_KEY], visited=visited
                    )
                else:
                    self._data[key] = await self._db.from_data(value, visited=visited)
            elif isinstance(value, list):
                for i in range(len(value)):
                    if isinstance(value[i], dict):
                        if pt.ID_KEY in value[i]:
                            value[i] = await self._db.load(
                                value[i][pt.ID_KEY], visited=visited
                            )
                        elif pt.TYPE_KEY in value[i]:
                            value[i] = await self._db.from_data(
                                value[i], visited=visited
                            )

    @classmethod
    def migrate(cls, data):
        version = data[pt.VERSION_KEY]
        while version != cls._version:
            if version < cls._version:
                version += 1
                data = cls.upgrade_to(data, version)
            else:
                data = cls.downgrade_from(data, version)
                version -= 1
        data[pt.VERSION_KEY] = cls._version
        return data

    @classmethod
    def upgrade_to(cls, data, version):
        return data

    @classmethod
    def downgrade_from(cls, data, version):
        return data


class Property:
    def __init__(
        self,
        proptype: t.Union[str, type, None] = None,
        default: t.Any = DEFAULT_NONE,
        volatile: bool = False,
    ):
        self.proptype_ = proptype
        self.default = default
        self.get_data = operator.attrgetter("_volatile" if volatile else "_data")

    def __set_name__(self, owner: type, name: str):
        self.name = name

    def __get__(self, instance: BaseThing, owner: type = None) -> t.Any:
        if instance is None:
            # Accessing Property on class, not instance
            return self
        if self.name not in self.get_data(instance):
            if self.default is not DEFAULT_NONE:
                self.get_data(instance)[self.name] = self.default
            else:
                self.get_data(instance)[self.name] = self.proptype()
        return self.get_data(instance)[self.name]

    def __set__(self, instance: BaseThing, value: t.Any):
        self.get_data(instance)[self.name] = self.typecheck(value)

    def __delete__(self, instance: BaseThing):
        try:
            del self.get_data(instance)[self.name]
        except KeyError:
            pass

    @property
    def proptype(self) -> t.Optional[type]:
        if isinstance(self.proptype_, str):
            self.proptype_ = get_thing_type(self.proptype_)
        return self.proptype_

    def typecheck(self, value: t.Any) -> t.Any:
        t = self.proptype
        if t is None:
            return value
        if value is None and self.default is None:
            return value
        if isinstance(value, t):
            return value
        raise ValueError(f"{self.name} must be of type {t}")
