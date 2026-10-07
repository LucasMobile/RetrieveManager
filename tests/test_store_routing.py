import unittest
from pathlib import Path

from app.store_routing import (
    StoreEndpoint,
    build_routing_plan,
    endpoint_conflicts,
    normalize_aet,
    units_sharing_listener,
)
from app.validation import store_allowed_senders, validate_store_allowed_aets

ROOT = Path("/data/store-routing")


def endpoint(unit_id, name=None, **overrides) -> StoreEndpoint:
    folder = ROOT / f"unit{unit_id}"
    values = {
        "unit_id": unit_id,
        "name": name or f"Empresa{unit_id}",
        "enabled": True,
        "store_port": 445,
        "calling_aet": "MOBILEMED",
        "store_allowed_aets": "",
        "receive_dir": str(folder / "receive"),
        "send_dir": str(folder / "send"),
        "error_dir": str(folder / "error"),
    }
    values.update(overrides)
    return StoreEndpoint.from_values(**values)


class RoutingPlanTest(unittest.TestCase):
    def test_sender_picks_the_unit_on_a_shared_port_and_called_aet(self):
        plan = build_routing_plan(
            [
                endpoint(1, store_allowed_aets="serverPacs1"),
                endpoint(2, store_allowed_aets="SERVERPACS2,backup"),
            ]
        )

        self.assertEqual(plan.lookup(445, "MOBILEMED", "SERVERPACS1"), 1)
        self.assertEqual(plan.lookup(445, "mobilemed ", " serverpacs2"), 2)
        self.assertEqual(plan.lookup(445, "MOBILEMED", "BACKUP"), 2)
        self.assertIsNone(plan.lookup(445, "MOBILEMED", "OUTRO"))
        self.assertTrue(plan.knows_listener(445, "mobilemed"))
        self.assertEqual(plan.unit_problems, {})

    def test_single_unit_with_empty_list_accepts_any_sender(self):
        plan = build_routing_plan([endpoint(1)])

        self.assertEqual(plan.lookup(445, "MOBILEMED", "QUALQUER"), 1)
        self.assertIsNone(plan.lookup(445, "OUTRO", "QUALQUER"))
        self.assertFalse(plan.knows_listener(445, "OUTRO"))

    def test_different_called_aets_on_the_same_port_are_separate_groups(self):
        plan = build_routing_plan([endpoint(1), endpoint(2, calling_aet="OUTROAET")])

        self.assertEqual(plan.lookup(445, "MOBILEMED", "X"), 1)
        self.assertEqual(plan.lookup(445, "OUTROAET", "X"), 2)
        self.assertEqual(plan.unit_problems, {})

    def test_empty_list_in_a_shared_group_is_never_a_catch_all(self):
        plan = build_routing_plan(
            [endpoint(1), endpoint(2, store_allowed_aets="SERVERPACS2")]
        )

        self.assertIsNone(plan.lookup(445, "MOBILEMED", "SERVERPACS1"))
        self.assertEqual(plan.lookup(445, "MOBILEMED", "SERVERPACS2"), 2)
        self.assertIn(1, plan.unit_problems)
        self.assertNotIn(2, plan.unit_problems)

    def test_sender_claimed_twice_is_refused_for_both_units(self):
        plan = build_routing_plan(
            [
                endpoint(1, store_allowed_aets="SERVERPACS1,COMUM"),
                endpoint(2, store_allowed_aets="SERVERPACS2,comum"),
            ]
        )

        self.assertIsNone(plan.lookup(445, "MOBILEMED", "COMUM"))
        self.assertTrue(plan.is_ambiguous(445, "MOBILEMED", "comum"))
        self.assertEqual(plan.lookup(445, "MOBILEMED", "SERVERPACS1"), 1)
        self.assertEqual(set(plan.unit_problems), {1, 2})

    def test_paused_units_keep_their_routing_keys(self):
        plan = build_routing_plan(
            [
                endpoint(1, enabled=False, store_allowed_aets="SERVERPACS1"),
                endpoint(2, store_allowed_aets="SERVERPACS2"),
            ]
        )

        # The receiver refuses unit 1 because it is paused; the key is
        # never handed to the neighbour.
        self.assertEqual(plan.lookup(445, "MOBILEMED", "SERVERPACS1"), 1)

    def test_shared_folders_block_both_units(self):
        plan = build_routing_plan(
            [
                endpoint(1, store_port=444),
                endpoint(2, receive_dir=str(ROOT / "unit1" / "receive" / "sub")),
                endpoint(3, store_port=446),
            ]
        )

        self.assertEqual(set(plan.unit_problems), {1, 2})

    def test_store_port_map_groups_external_and_bind_ports(self):
        a = endpoint(1, store_port=444)
        b = endpoint(2, store_port=10444)

        self.assertEqual(a.port, b.port)
        self.assertEqual(units_sharing_listener(a, [a, b]), [b])


class EndpointConflictsTest(unittest.TestCase):
    def test_disjoint_lists_on_a_shared_port_are_accepted(self):
        others = [endpoint(1, store_allowed_aets="SERVERPACS1")]

        self.assertEqual(
            endpoint_conflicts(endpoint(2, store_allowed_aets="SERVERPACS2"), others),
            [],
        )

    def test_shared_port_needs_a_list_on_both_sides(self):
        mine = endpoint(2, store_allowed_aets="SERVERPACS2")
        (problem,) = endpoint_conflicts(mine, [endpoint(1)])
        self.assertIn("Empresa1", problem)

        (problem,) = endpoint_conflicts(
            endpoint(2), [endpoint(1, store_allowed_aets="SERVERPACS1")]
        )
        self.assertIn("AE Titles autorizados", problem)

    def test_overlapping_lists_name_the_sender_and_the_unit(self):
        (problem,) = endpoint_conflicts(
            endpoint(2, store_allowed_aets="serverpacs1"),
            [endpoint(1, store_allowed_aets="SERVERPACS1")],
        )

        self.assertIn("SERVERPACS1", problem)
        self.assertIn("Empresa1", problem)

    def test_folders_must_not_match_or_nest_with_another_unit(self):
        other = endpoint(1, store_port=444)
        same = endpoint(2, receive_dir=str(ROOT / "unit1" / "send" / ".." / "receive"))
        nested = endpoint(3, send_dir=str(ROOT / "unit1" / "send" / "out"))
        parent = endpoint(4, error_dir=str(ROOT / "unit1"))

        self.assertEqual(len(endpoint_conflicts(same, [other])), 1)
        self.assertEqual(len(endpoint_conflicts(nested, [other])), 1)
        # A parent folder holds all three folders of the other unit.
        self.assertEqual(len(endpoint_conflicts(parent, [other])), 3)
        self.assertEqual(endpoint_conflicts(endpoint(5), [other]), [])

    def test_the_unit_itself_is_ignored_when_editing(self):
        mine = endpoint(1)

        self.assertEqual(endpoint_conflicts(mine, [endpoint(1)]), [])

    def test_paused_units_are_part_of_the_check(self):
        self.assertTrue(endpoint_conflicts(endpoint(2), [endpoint(1, enabled=False)]))


class NormalizationTest(unittest.TestCase):
    def test_form_receiver_and_compaction_compare_aets_the_same_way(self):
        stored = validate_store_allowed_aets(" serverPacs1 ; ServerPacs1,pacs-2 ")

        self.assertEqual(stored, "serverPacs1,pacs-2")
        self.assertEqual(store_allowed_senders(stored), {"SERVERPACS1", "PACS-2"})
        self.assertEqual(
            endpoint(1, store_allowed_aets=stored).senders,
            store_allowed_senders(stored),
        )
        self.assertEqual(normalize_aet("  pacs-2 "), "PACS-2")


if __name__ == "__main__":
    unittest.main()
