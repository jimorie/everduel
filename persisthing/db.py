from __future__ import annotations
import typing as t
import weakref

import persisthing as pt


class ThingsDB:
    def __init__(self, backend: pt.ThingsBackend):
        self.backend = backend
        self.cache = weakref.WeakValueDictionary()

    def create(self, thing_cls, **kwargs):
        return thing_cls(self, **kwargs)

    async def save(self, thing: pt.BaseThing) -> t.Any:
        thing.set_db(self)
        thing._data[pt.TYPE_KEY] = thing._type
        thing._data[pt.VERSION_KEY] = thing._version
        if thing._id:
            await self.backend.update(thing._id, thing._data)
        else:
            thing._id = await self.backend.create(thing._data)
            self.cache[thing._id] = thing
        return thing._id

    async def load(self, thing_id: int, visited: set = None) -> pt.BaseThing:
        if thing_id in self.cache:
            return self.cache[thing_id]
        data = await self.backend.load(thing_id)
        if data is None:
            raise pt.ThingDoesNotExistException()
        thing = await self.from_data(data, visited)
        thing._id = thing_id
        self.cache[thing_id] = thing
        return thing

    async def from_data(self, data: dict, visited: set = None) -> pt.BaseThing:
        thing_cls = pt.get_thing_type(data[pt.TYPE_KEY])
        version = data[pt.VERSION_KEY]
        if version != thing_cls._version:
            data = thing_cls.migrate(data)
        thing = thing_cls(self, _data=data)
        await thing.load_props(visited)
        return thing

    async def delete(self, thing: pt.BaseThing):
        if thing._id:
            await self.backend.delete(thing._id)
            thing._id = None

    async def clear(self):
        await self.backend.clear()

    async def close(self):
        self.cache.clear()
        return await self.backend.close()
