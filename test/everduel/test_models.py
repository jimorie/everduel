import everduel.models as m


def test_move():
    t1 = m.Thing()
    t2 = m.Thing()
    c1 = m.Container()
    c2 = m.Container()
    assert t1.container is None
    assert c1.things == []
    t1.move(c1)
    assert t1.container is c1
    assert c1.things == [t1]
    t2.move(c1)
    assert t2.container is c1
    assert c1.things == [t1, t2]
    t1.move(c2)
    assert t1.container == c2
    assert c2.things == [t1]
    assert c1.things == [t2]


def test_cyclical_move():
    a1 = m.Actor()
    a2 = m.Actor()
    assert a1.container is None
    assert a1.things == []
    assert a2.move(a1)
    assert a2.container is a1
    assert a1.things == [a2]
    assert not a1.move(a2)
    assert a1.container is None
    assert a2.things == []
    a3 = m.Actor()
    assert a3.move(a2)
    assert a3.container is a2
    assert a2.things == [a3]
    assert not a1.move(a3)
    assert a1.container is None
    assert a3.things == []
