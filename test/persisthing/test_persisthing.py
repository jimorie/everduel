import pathlib
import pytest
import pytest_asyncio

import persisthing as pt

TEST_DB = pathlib.Path(__file__).parent / "test.db"
TEST_DB_TABLE = "things"
TEST_TMP_DIR = pathlib.Path(__file__).parent / "tmp-file-backend-data"

# Handle all test coroutines as marked with asyncio
pytestmark = pytest.mark.asyncio


@pt.thing("test-t")
class MyThing(pt.BaseThing):
    name = pt.prop(str)
    price = pt.prop(int, default=10)
    negligible = pt.prop("test-t", volatile=True)


@pt.thing("test-p")
class MyPlayer(MyThing):
    thing = pt.prop(MyThing, default=None)
    buddy = pt.prop("test-p", default=None)
    inventory = pt.prop(list)


@pt.thing("test-upgraded")
class MyUpgradedThing(pt.BaseThing):
    _version = 3
    name = pt.prop(str)
    price = pt.prop(int, default=10)

    @classmethod
    def upgrade_to(cls, data, version):
        if version == 3:
            data["price"] = len(data["name"])
        return data

    @classmethod
    def downgrade_from(cls, data, version):
        if version == 3:
            del data["price"]
        return data


@pytest_asyncio.fixture(scope="function")
async def thingsdb():
    db = pt.ThingsDB(await pt.SqliteBackend.connect(TEST_DB, TEST_DB_TABLE))
    # db = pt.ThingsDB(await pt.FileBackend.connect(TEST_TMP_DIR))
    try:
        await db.clear()
        yield db
    finally:
        await db.close()


async def test_save_thing(thingsdb):
    thing = MyThing(name="Kaka")
    thing_id = await thing.save(thingsdb)
    r_data = await thingsdb.backend.load(thing_id)
    assert r_data == {
        pt.VERSION_KEY: 1,
        pt.TYPE_KEY: "test-t",
        "name": "Kaka",
    }
    return thing_id


async def test_load_thing(thingsdb):
    thing_id = await test_save_thing(thingsdb)
    thing = await thingsdb.load(thing_id)
    assert type(thing) is MyThing
    assert thing.name == "Kaka"
    return thing


async def test_update_thing(thingsdb):
    thing = await test_load_thing(thingsdb)
    thing.name = "Bulle"
    await thing.save()
    r_data = await thingsdb.backend.load(thing._id)
    assert r_data["name"] == "Bulle"


async def test_weakref_cache(thingsdb):
    assert len(thingsdb.cache) == 0
    thing_id = await test_save_thing(thingsdb)
    assert len(thingsdb.cache) == 0
    thing = await thingsdb.load(thing_id)
    assert len(thingsdb.cache) == 1
    del thing
    assert len(thingsdb.cache) == 0
    thing1 = await thingsdb.load(thing_id)
    thing2 = await thingsdb.load(thing_id)
    assert thing1 is thing2
    assert len(thingsdb.cache) == 1
    del thing1
    assert len(thingsdb.cache) == 1
    del thing2
    assert len(thingsdb.cache) == 0


async def test_save_player(thingsdb):
    player = MyPlayer(name="Alice")
    assert player._data == {
        "name": "Alice",
    }
    await player.save(thingsdb)
    r_data = await thingsdb.backend.load(player._id)
    assert r_data == {pt.TYPE_KEY: "test-p", pt.VERSION_KEY: 1, "name": "Alice"}
    return player._id


async def test_load_player(thingsdb):
    player_id = await test_save_player(thingsdb)
    player = await thingsdb.load(player_id)
    assert type(player) is MyPlayer
    assert player.name == "Alice"
    return player


async def test_save_player_thing_inline(thingsdb):
    player = await test_load_player(thingsdb)
    thing = MyThing(thingsdb, name="Bulle")
    player.thing = thing
    await player.save()
    r_data = await thingsdb.backend.load(player._id)
    assert r_data == {
        pt.TYPE_KEY: "test-p",
        pt.VERSION_KEY: 1,
        "name": "Alice",
        "thing": {
            pt.TYPE_KEY: "test-t",
            pt.VERSION_KEY: 1,
            "name": "Bulle",
        },
    }
    return player._id


async def test_load_player_thing_inline(thingsdb):
    player_id = await test_save_player_thing_inline(thingsdb)
    player = await thingsdb.load(player_id)
    assert type(player.thing) is MyThing
    assert player.thing.name == "Bulle"
    return player


async def test_save_player_thing_reference(thingsdb):
    player = await test_load_player_thing_inline(thingsdb)
    await player.thing.save()
    await player.save()
    r_data = await thingsdb.backend.load(player._id)
    assert r_data == {
        pt.TYPE_KEY: "test-p",
        pt.VERSION_KEY: 1,
        "name": "Alice",
        "thing": {
            pt.ID_KEY: player.thing._id,
        },
    }
    return player._id


async def test_load_player_thing_reference(thingsdb):
    player_id = await test_save_player_thing_reference(thingsdb)
    assert player_id not in thingsdb.cache
    player = await thingsdb.load(player_id)
    assert player_id in thingsdb.cache
    assert player.thing._id in thingsdb.cache
    assert type(player.thing) is MyThing
    assert player.thing.name == "Bulle"
    return player


async def test_default(thingsdb):
    thing = MyThing(thingsdb)
    assert thing.price == 10
    player = MyPlayer(thingsdb)
    player.inventory.append(thing)
    assert player.inventory == [thing]


async def test_save_player_thing_inline_in_list(thingsdb):
    player = await test_load_player(thingsdb)
    thing = MyThing(thingsdb)
    thing.name = "Saft"
    player.inventory.append(thing)
    await player.save()
    r_data = await thingsdb.backend.load(player._id)
    assert r_data == {
        pt.TYPE_KEY: "test-p",
        pt.VERSION_KEY: 1,
        "name": "Alice",
        "inventory": [
            {
                pt.TYPE_KEY: "test-t",
                pt.VERSION_KEY: 1,
                "name": "Saft",
            }
        ],
    }
    return player._id


async def test_load_player_thing_inline_in_list(thingsdb):
    player_id = await test_save_player_thing_inline_in_list(thingsdb)
    player = await thingsdb.load(player_id)
    assert len(player.inventory) == 1
    assert type(player.inventory[0]) is MyThing
    assert player.inventory[0].name == "Saft"
    return player


async def test_save_player_thing_reference_in_list(thingsdb):
    player = await test_load_player_thing_inline_in_list(thingsdb)
    thing2 = MyThing(thingsdb)
    thing2.name = "Bärs"
    player.inventory.append(thing2)
    await thing2.save()
    await player.save()
    r_data = await thingsdb.backend.load(player._id)
    assert r_data == {
        pt.TYPE_KEY: "test-p",
        pt.VERSION_KEY: 1,
        "name": "Alice",
        "inventory": [
            {
                pt.TYPE_KEY: "test-t",
                pt.VERSION_KEY: 1,
                "name": "Saft",
            },
            {
                pt.ID_KEY: thing2._id,
            },
        ],
    }
    return player._id


async def test_load_player_thing_reference_in_list(thingsdb):
    player_id = await test_save_player_thing_reference_in_list(thingsdb)
    player = await thingsdb.load(player_id)
    assert len(player.inventory) == 2
    assert type(player.inventory[0]) is MyThing
    assert type(player.inventory[1]) is MyThing
    assert player.inventory[0].name == "Saft"
    assert player.inventory[1].name == "Bärs"
    assert player.inventory[0]._id is None
    assert player.inventory[1]._id is not None
    return player


async def test_cyclical_prop(thingsdb):
    player = MyPlayer(thingsdb)
    player.name = "Alice"
    player.buddy = player
    with pytest.raises(ValueError):
        await player.save()
    del player

    player1 = MyPlayer(thingsdb)
    player1.name = "Alice"
    player2 = MyPlayer(thingsdb)
    player2.name = "Bob"
    player1.buddy = player2
    player2.buddy = player1
    assert player1.buddy.buddy.buddy.buddy is player1
    assert player2.buddy.buddy.buddy.buddy.buddy is player1
    with pytest.raises(ValueError):
        await player1.save()
    with pytest.raises(ValueError):
        await player2.save()
    del player1
    del player2

    # If you save *first* it actually works...
    player1 = MyPlayer(thingsdb)
    player1.name = "Alice"
    await player1.save()
    player2 = MyPlayer(thingsdb)
    player2.name = "Bob"
    await player2.save()
    player1.buddy = player2
    player2.buddy = player1
    await player1.save()
    await player2.save()
    r_data = await thingsdb.backend.load(player1._id)
    assert r_data == {
        pt.TYPE_KEY: "test-p",
        pt.VERSION_KEY: 1,
        "name": "Alice",
        "buddy": {pt.ID_KEY: player2._id},
    }


async def test_volatile():
    thing1 = MyThing(name="Kaka")
    thing2 = MyThing(name="Bulle", negligible=thing1)
    assert thing2._data == {
        "name": "Bulle",
    }
    assert thing2._volatile == {"negligible": thing1}


async def test_migration(thingsdb):
    thing = MyUpgradedThing(name="Kaka", price=42)
    thing_id = await thing.save(thingsdb)
    assert thing._data == {
        pt.TYPE_KEY: "test-upgraded",
        pt.VERSION_KEY: 3,
        "name": "Kaka",
        "price": 42,
    }
    assert thing.price == 42
    del thing
    # Downgrade
    MyUpgradedThing._version = 1
    price_prop = MyUpgradedThing.price
    del MyUpgradedThing.price
    thing = await thingsdb.load(thing_id)
    assert thing._data == {
        pt.TYPE_KEY: "test-upgraded",
        pt.VERSION_KEY: 1,
        "name": "Kaka",
    }
    assert hasattr(thing, "price") is False
    thing_id = await thing.save(thingsdb)
    del thing
    # Upgrade
    MyUpgradedThing._version = 3
    MyUpgradedThing.price = price_prop
    thing = await thingsdb.load(thing_id)
    assert thing._data == {
        pt.TYPE_KEY: "test-upgraded",
        pt.VERSION_KEY: 3,
        "name": "Kaka",
        "price": len("Kaka"),
    }
