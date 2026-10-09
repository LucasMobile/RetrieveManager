import unittest

from sqlalchemy import select

from app.events import add_event
from app.models import Order, OrderEvent
from tests.support import DatabaseTestCase, make_unit


class EventCollapseTest(DatabaseTestCase):
    def _order(self, db) -> Order:
        unit = make_unit(name="unit")
        db.add(unit)
        db.flush()
        order = Order(unit_id=unit.id, source_id="x", acc="1", birth_date="20000101")
        db.add(order)
        db.flush()
        return order

    def events(self, db) -> list[OrderEvent]:
        return list(db.scalars(select(OrderEvent).order_by(OrderEvent.id)))

    def test_repeated_routine_event_shares_one_row(self):
        with self.Session() as db:
            order = self._order(db)
            for number in range(1, 4):
                add_event(db, order, f"Verificação {number}", kind="poll")
            db.commit()

            (event,) = self.events(db)
            self.assertEqual(event.message, "Verificação 3")
            self.assertEqual(event.repeat_count, 3)
            self.assertIsNotNone(event.last_seen_at)

    def test_another_event_in_between_starts_a_new_row(self):
        with self.Session() as db:
            order = self._order(db)
            add_event(db, order, "C-FIND vazio", kind="poll")
            add_event(db, order, "Exame encontrado")
            add_event(db, order, "C-FIND vazio", kind="poll")
            add_event(db, order, "C-FIND falhou", level="warn", kind="poll")
            db.commit()

            self.assertEqual(
                [(e.message, e.repeat_count) for e in self.events(db)],
                [
                    ("C-FIND vazio", 1),
                    ("Exame encontrado", 1),
                    ("C-FIND vazio", 1),
                    ("C-FIND falhou", 1),
                ],
            )

    def test_events_without_kind_never_collapse(self):
        with self.Session() as db:
            order = self._order(db)
            add_event(db, order, "Pedido confirmado")
            add_event(db, order, "Pedido confirmado")
            db.commit()
            self.assertEqual(len(self.events(db)), 2)


if __name__ == "__main__":
    unittest.main()
