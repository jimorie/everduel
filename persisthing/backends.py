from __future__ import annotations
import json
import pathlib
import typing as t
import uuid

import aiosqlite
import sqlite3

import persisthing as pt


class ThingJsonEncoder(json.JSONEncoder):
    def default(self, thing):
        if isinstance(thing, pt.BaseThing):
            if thing._id:
                return {pt.ID_KEY: thing._id}
            thing._data[pt.TYPE_KEY] = thing._type
            thing._data[pt.VERSION_KEY] = thing._version
            return thing._data
        return thing


class ThingsBackend:
    encoder_cls = ThingJsonEncoder
    decoder_cls = json.JSONDecoder

    def __init__(self):
        self.encoder = self.encoder_cls()
        self.decoder = self.decoder_cls()

    @classmethod
    async def connect(cls, *args, **kwargs) -> ThingsBackend:
        raise NotImplementedError

    async def create(self, thing_type: str, data: dict, version: int) -> t.Any:
        raise NotImplementedError

    async def update(self, thing_id: int, data: dict):
        raise NotImplementedError

    async def delete(self, thing_id: int):
        raise NotImplementedError

    async def clear(self):
        raise NotImplementedError

    async def load(self, thing_id: int) -> t.Optional[dict]:
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError


class SqliteBackend(ThingsBackend):
    def __init__(self, conn: aiosqlite.Connection, tablename: str):
        self._conn = conn
        self._loadsql = f"SELECT data FROM {tablename} WHERE id = :id"
        self._deletesql = f"DELETE FROM {tablename} WHERE id = :id"
        self._clearsql = f"DELETE FROM {tablename}"
        self._createsql = f"INSERT INTO {tablename} (data) VALUES (:data)"
        self._updatesql = f"UPDATE {tablename} SET data = :data WHERE id = :id"
        super().__init__()

    @classmethod
    async def connect(
        cls, dbpath: t.Union[str, pathlib.Path], tablename: str
    ) -> ThingsBackend:
        conn = await aiosqlite.connect(dbpath)
        backend = cls(conn, tablename)
        await backend.initdb(tablename)
        return backend

    async def create(self, data: dict) -> int:
        cursor = await self.execute(self._createsql, data=self.encoder.encode(data))
        await self.commit()
        await cursor.close()
        return cursor.lastrowid

    async def update(self, thing_id: int, data: dict):
        cursor = await self.execute(
            self._updatesql, id=thing_id, data=self.encoder.encode(data)
        )
        await self.commit()
        await cursor.close()

    async def delete(self, thing_id: int):
        cursor = await self.execute(self._deletesql, id=thing_id)
        await self.commit()
        await cursor.close()

    async def load(self, thing_id: int) -> t.Optional[dict]:
        result = await self.fetchone(self._loadsql, id=thing_id)
        return result and self.decoder.decode(result[0])

    async def clear(self):
        cursor = await self.execute(self._clearsql)
        await self.commit()
        await cursor.close()

    async def close(self):
        return await self._conn.close()

    async def initdb(self, tablename: str):
        await self.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {tablename} (
                id INTEGER PRIMARY KEY,
                data BLOB NOT NULL
            );
            """
        )

    async def execute(self, sql: str, **params) -> aiosqlite.cursor.Cursor:
        return await self._conn.execute(sql, params)

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

    async def commit(self):
        return await self._conn.commit()


class FileBackend(ThingsBackend):
    def __init__(self, directory: t.Union[str, pathlib.Path]):
        self.encoder = self.encoder_cls(indent=4)
        self.decoder = self.decoder_cls()
        if isinstance(directory, str):
            self.directory = pathlib.Path(directory)
        else:
            self.directory = directory
        self.directory.mkdir(exist_ok=True)

    @classmethod
    async def connect(cls, directory: t.Union[str, pathlib.Path]) -> ThingsBackend:
        return cls(directory)

    async def create(self, data: dict) -> str:
        thing_id = str(uuid.uuid4())
        await self.update(thing_id, data)
        return thing_id

    async def update(self, thing_id: str, data: dict):
        file_path = self.directory / thing_id
        try:
            with open(file_path, "w") as fd:
                for chunk in self.encoder.iterencode(data):
                    fd.write(chunk)
        except ValueError:
            if file_path.is_file():
                file_path.unlink()
            raise

    async def delete(self, thing_id: str):
        file_path = self.directory / thing_id
        if file_path.is_file():
            file_path.unlink()

    async def load(self, thing_id: int) -> t.Optional[str]:
        with open(self.directory / thing_id, "r") as fd:
            return self.decoder.decode(fd.read())

    async def clear(self):
        for file_path in self.directory.glob("*"):
            file_path.unlink()

    async def close(self):
        pass
