import asyncio
import sys

import everdata.everdata as e

async def test1(db):
    # t = e.Thing(db, {"name": "Olle"})
    # await t.save()
    alice = e.Player(db)
    alice.name = "Alice"
    bob = e.Player(db)
    bob.name = "Bob"
    bob.target = alice
    await alice.save()
    await bob.save()
    import pdb; pdb.set_trace()

async def test2(db):
    alice = await db.load(18)
    bob = await db.load(19)
    import pdb; pdb.set_trace()

async def main():
    db = await e.ThingDatabase.connect("foo.db", "things")
    await test2(db)
    await db.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    sys.exit(0)
