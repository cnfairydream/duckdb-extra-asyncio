# -*- coding: utf-8 -*-

import asyncio
from asyncio import Queue, Future
from typing import Callable

import duckdb
from duckdb import DuckDBPyConnection

from .utils import delegate_to_agent, proxy_property_directly


class Agent:

    def __init__(self):
        self.worker = None
        self.running = False
        self.queue = Queue()
        self.loop = asyncio.get_running_loop()

    async def submit(self, func: Callable, *args, **kwargs) -> Future:
        future = self.loop.create_future()
        await self.queue.put((future, func, args, kwargs))
        return future

    async def start(self):
        if not self.running:
            self.running = True
            self.worker = asyncio.create_task(self.run())

    async def stop(self):
        if self.running:
            self.running = False
            await self.worker

    async def run(self):
        while True:
            if not self.running and self.queue.empty():
                break

            elif self.running and self.queue.empty():
                await asyncio.sleep(0)

            else:
                future, func, args, kwargs = await self.queue.get()

                try:
                    result = await asyncio.to_thread(func, *args, **kwargs)
                    future.set_result(result)
                except BaseException as e:
                    future.set_exception(e)

                self.queue.task_done()


@delegate_to_agent(
    "begin",
    "commit",
    "rollback",
    "checkpoint",
    "load_extension",
    "create_function",
    "remove_function",
)
@proxy_property_directly("rowcount")
class AsyncConnection:

    def __init__(self, database: str, config: dict = None):
        self._connection = duckdb.connect(database, config=config or {})
        self._agent = Agent()

    async def __aenter__(self):
        if not self._agent.running:
            await self._agent.start()

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

        if self._agent.running:
            await self._agent.stop()

    async def _execute(self, func, *args, **kwargs):
        if not self._agent.running:
            await self._agent.start()

        future = await self._agent.submit(func, *args, **kwargs)
        await future

        return future.result()

    def cursor(self):
        return AsyncCursor(self, self._connection.cursor())

    async def close(self):
        await self._execute(self._connection.close)

        if self._agent.running:
            await self._agent.stop()

    async def execute(self, query: str, parameters=None):
        cursor = await self._execute(self._connection.execute, query, parameters)
        return AsyncCursor(self, cursor)

    async def executemany(self, query: str, parameters=None):
        cursor = await self._execute(self._connection.executemany, query, parameters)
        return AsyncCursor(self, cursor)


@delegate_to_agent(
    "close",
    "execute",
    "executemany",
    "fetchall",
    "fetchdf",
    "fetchmany",
    "fetchnumpy",
    "fetchone",
    "fetch_arrow_table",
    "fetch_df",
    "fetch_df_chunk",
    "fetch_record_batch",
    "df",
    "pl",
    "tf",
    "torch",
)
class AsyncCursor:

    def __init__(self, connection: AsyncConnection, cursor: DuckDBPyConnection | None = None):
        self._connection = cursor or getattr(connection, "_connection").cursor()
        self._agent = getattr(connection, "_agent")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def __aiter__(self):
        while True:
            rows = await self.fetchmany(size=100)
            if not rows:
                return

            for row in rows:
                yield row

    async def _execute(self, func, *args, **kwargs):
        future = await self._agent.submit(func, *args, **kwargs)
        await future
        return future.result()
