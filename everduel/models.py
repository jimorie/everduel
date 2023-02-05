from __future__ import annotations

import persisthing as pt


@pt.thing("C")
class Container(pt.BaseThing):
    things = pt.prop(list)

    def receive(self, thing: "Thing") -> bool:
        self.things.append(thing)
        return True

    def release(self, thing: "Thing") -> bool:
        self.things.remove(thing)
        return True


@pt.thing("T")
class Thing(pt.BaseThing):
    name = pt.prop(str)
    container = pt.prop(Container, default=None)

    def move(self, container: Container) -> bool:
        if isinstance(container, Container):
            if self.container is None or self.container.release(self):
                self.container = None
                if container.receive(self):
                    self.container = container
                    return True
        return False


class ContainerThing(Container, Thing):
    def move(self, container: Container) -> bool:
        parent = container
        while isinstance(parent, Thing):
            if parent is self:
                return False
            parent = parent.container
        return super().move(container)


@pt.thing("W")
class Weapon(Thing):
    damage = pt.prop(int)


@pt.thing("stats")
class Stats(pt.BaseThing):
    might = pt.prop(int)
    skill = pt.prop(int)
    cunning = pt.prop(int)
    empathy = pt.prop(int)
    armor = pt.prop(int)
    health = pt.prop(int)


@pt.thing("race")
class Race(pt.BaseThing):
    name = pt.prop(str)
    stats = pt.prop(Stats)


@pt.thing("eq")
class Equipment(Thing):
    pass


@pt.thing("held")
class Held(Equipment):
    pass


@pt.thing("helm")
class Headwear(Equipment):
    pass


@pt.thing("armor")
class Armor(Equipment):
    pass


@pt.thing("ring")
class Ring(Equipment):
    pass


@pt.thing("slots")
class Slots(pt.BaseThing):
    hand1 = pt.prop(Held)
    hand2 = pt.prop(Held)
    head = pt.prop(Headwear)
    armor = pt.prop(Armor)
    ring1 = pt.prop(Ring)
    ring2 = pt.prop(Ring)


@pt.thing("A")
class Actor(ContainerThing):
    race = pt.prop(Race)
    stats = pt.prop(Stats)
    slots = pt.prop(Slots)
    weapon = pt.prop(Weapon)
    target = pt.prop("A")
