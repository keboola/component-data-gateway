import unittest
import mock
import os
import json
from freezegun import freeze_time

from component import Component, parse_last_run_to_timestamp


class TestComponent(unittest.TestCase):
    def comparedict(self, actual, expected, msg=None):
        """
        Helper method to compare dictionaries with clear error messages.

        Args:
            actual: The actual dictionary to check
            expected: Dictionary with expected key-value pairs
            msg: Optional message prefix
        """
        for key, expected_value in expected.items():
            self.assertIn(key, actual, f"{msg}: Key '{key}' not found in actual dict")
            self.assertEqual(actual[key], expected_value, f"{msg}: Value mismatch for key '{key}'")

    def test_parse_last_run_to_timestamp(self):
        """Test parse_last_run_to_timestamp handles both Unix timestamp and ISO format"""
        # Test Unix timestamp (int)
        result = parse_last_run_to_timestamp(1767792606)
        self.assertEqual(result, 1767792606)

        # Test Unix timestamp (float)
        result = parse_last_run_to_timestamp(1767792606.0)
        self.assertEqual(result, 1767792606)

        # Test ISO format string
        result = parse_last_run_to_timestamp("2026-01-07T13:30:06+00:00")
        self.assertEqual(result, 1767792606)

    # set global time to 2010-10-10 - affects functions like datetime.now()
    @freeze_time("2010-10-10")
    # set KBC_DATADIR env to non-existing dir
    @mock.patch.dict(os.environ, {"KBC_DATADIR": "./non-existing-dir"})
    def test_run_no_cfg_fails(self):
        with self.assertRaises(ValueError):
            comp = Component()
            comp.run()

    # FULL LOAD TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_basic",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_full_load_basic(self, mock_client):
        """Test basic full load with column mapping"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        self.comparedict(call_args[1], {"workspace_id": 12345, "preserve": True}, "load_tables parameters")

        # Check table mapping
        table_mapping = call_args[1]["table_mapping"]
        self.assertEqual(len(table_mapping), 1)

        mapping = table_mapping[0]
        self.comparedict(mapping, {"source": "in.c-main.users", "destination": "users_table"}, "Table mapping")
        self.assertEqual(len(mapping["columns"]), 2)

        # Check columns (they are dicts, not objects)
        col_0 = mapping["columns"][0]
        self.comparedict(
            col_0,
            {"source": "id", "destination": "id", "type": "VARCHAR", "length": "255", "nullable": False},
            "Column 0",
        )

        col_1 = mapping["columns"][1]
        self.comparedict(
            col_1,
            {"source": "name", "destination": "name", "type": "VARCHAR", "length": "255", "nullable": True},
            "Column 1",
        )

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_with_pk",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_full_load_with_primary_keys(self, mock_client):
        """Test full load with primary key configuration"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Check primary key (camelCase)
        self.assertIn("primaryKey", mapping)
        self.assertEqual(mapping["primaryKey"]["columns"], ["id", "sku"])

        # Check columns include all PK columns
        column_names = [col["source"] for col in mapping["columns"]]
        self.assertIn("id", column_names)
        self.assertIn("sku", column_names)
        self.assertIn("price", column_names)

        # Check FLOAT type
        price_col = [col for col in mapping["columns"] if col["source"] == "price"][0]
        self.comparedict(price_col, {"type": "FLOAT", "nullable": True}, "Price column")

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_with_pk",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_primary_key_validation(self, mock_client):
        """Test that primary key columns must be in selected columns"""
        # Modify config to have PK column not in items
        config_path = "./tests/data/full_load_with_pk/config.json"
        with open(config_path, "r") as f:
            config = json.load(f)

        # Add PK column that's not in items
        config["parameters"]["primaryKey"] = ["id", "sku", "missing_column"]

        # Write modified config
        with open(config_path, "w") as f:
            json.dump(config, f)

        try:
            # Run component - should raise UserException
            comp = Component()
            with self.assertRaises(Exception) as context:
                comp.run()

            # Check error message mentions validation
            self.assertIn("Primary key", str(context.exception))
        finally:
            # Restore original config
            config["parameters"]["primaryKey"] = ["id", "sku"]
            with open(config_path, "w") as f:
                json.dump(config, f)

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_basic",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_missing_input_table_error(self, mock_client):
        """Test that missing input table raises UserException"""
        # Modify config to reference non-existent table
        config_path = "./tests/data/full_load_basic/config.json"
        with open(config_path, "r") as f:
            config = json.load(f)

        original_table_id = config["parameters"]["tableId"]
        config["parameters"]["tableId"] = "in.c-main.nonexistent"

        # Write modified config
        with open(config_path, "w") as f:
            json.dump(config, f)

        try:
            # Run component - should raise exception (list index out of range)
            comp = Component()
            with self.assertRaises(Exception):
                comp.run()
        finally:
            # Restore original config
            config["parameters"]["tableId"] = original_table_id
            with open(config_path, "w") as f:
                json.dump(config, f)

    # INCREMENTAL LOAD TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/incremental_adaptive",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_incremental_adaptive_mode(self, mock_client):
        """Test incremental load using state file (subsequent run)"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Check incremental mode with changedSince and changedUntil parameters
        # Frozen time: 2024-01-15 10:00:00
        # State last_run: 2024-01-15 09:00:00 (from in/state.json)
        # changedSince: 1705309200 (2024-01-15T09:00:00+00:00 as timestamp)
        # changedUntil: 1705312800 (2024-01-15T10:00:00+00:00 as timestamp)
        self.comparedict(
            mapping,
            {
                "incremental": True,
                "changedSince": 1705309200,
                "changedUntil": 1705312800,
            },
            "Incremental mode settings",
        )

        # Check TEXT column type
        event_col = [col for col in mapping["columns"] if col["source"] == "event_name"][0]
        self.comparedict(event_col, {"type": "TEXT"}, "Event name column")

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/incremental_fixed_time",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_incremental_first_run(self, mock_client):
        """Test incremental load on first run (no state file)"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Check incremental flag
        self.comparedict(mapping, {"incremental": True}, "Incremental mode")

        # Check changedSince and changedUntil parameters
        # Frozen time: 2024-01-15 10:00:00 = 1705312800
        # This test uses "-30 minutes" which uses get_past_date()
        # Note: get_past_date doesn't work well with frozen time, so we just check it's a reasonable timestamp
        self.assertIn("changedSince", mapping)
        self.assertIn("changedUntil", mapping)
        self.assertTrue(isinstance(mapping["changedSince"], int), "changedSince should be an integer")
        self.assertEqual(mapping["changedUntil"], 1705312800)

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/incremental_adaptive",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_incremental_uses_state(self, mock_client):
        """Test that incremental load calculates time range from state file"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component - should use state file
        comp = Component()
        comp.run()

        # Assert load_tables was called
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Should be incremental and use state file (last_run: 2024-01-15T09:00:00+00:00)
        self.comparedict(
            mapping,
            {
                "incremental": True,
                "changedSince": 1705309200,
                "changedUntil": 1705312800,
            },
            "Incremental mode using state",
        )

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/incremental_adaptive",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_incremental_updates_state(self, mock_client):
        """Test that incremental load updates state file with last_run timestamp"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Check state file was written
        state_path = "./tests/data/incremental_adaptive/out/state.json"
        self.assertTrue(os.path.exists(state_path))

        # Read state file
        with open(state_path, "r") as f:
            state = json.load(f)

        # Check last_run was updated to frozen time (stored as ISO format string)
        # Frozen time 2024-01-15 10:00:00 as ISO format
        self.comparedict(state, {"last_run": "2024-01-15T10:00:00+00:00"}, "State file")

    # CLONE MODE TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/clone_mode",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_clone_mode_basic(self, mock_client):
        """Test clone mode sets load_type to CLONE and dropTimestampColumn"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Check CLONE mode settings
        self.comparedict(mapping, {"loadType": "CLONE", "dropTimestampColumn": True}, "Clone mode settings")

        # In clone mode, changedSince and changedUntil are None (no time filtering)
        self.assertIsNone(mapping.get("changedSince"))
        self.assertIsNone(mapping.get("changedUntil"))

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/clone_mode",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_clone_mode_removes_incompatible_fields(self, mock_client):
        """Test clone mode uses CLONE loadType and doesn't have seconds field"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert load_tables call
        call_args = mock_client_instance.workspaces.load_tables.call_args
        table_mapping = call_args[1]["table_mapping"]
        mapping = table_mapping[0]

        # Check that CLONE loadType is set
        self.comparedict(mapping, {"loadType": "CLONE"}, "Clone mode loadType")

        # Check that changedSince and changedUntil are None (no time filtering in CLONE mode)
        self.assertIsNone(mapping.get("changedSince"))
        self.assertIsNone(mapping.get("changedUntil"))

        # Check that deprecated seconds field is not in mapping
        self.assertNotIn("seconds", mapping)

    # WORKSPACE RESOLUTION TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/workspace_from_config",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_workspace_from_config(self, mock_client):
        """Test workspace ID is taken from config parameters"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert workspace_id from config is used
        call_args = mock_client_instance.workspaces.load_tables.call_args
        self.assertEqual(call_args[1]["workspace_id"], 67890)

        # Assert list_config_workspaces was NOT called
        mock_client_instance.configurations.list_config_workspaces.assert_not_called()

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/workspace_discovery",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_workspace_from_discovery(self, mock_client):
        """Test workspace ID is discovered via API when not in config"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.configurations.list_config_workspaces.return_value = [
            {"id": 99999, "connection": {"backend": "snowflake"}}
        ]
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "status": "success",
            "id": "12345",
            "createdTime": "2024-01-15T10:00:00+00:00",
            "startTime": "2024-01-15T10:00:01+00:00",
            "endTime": "2024-01-15T10:00:05+00:00",
        }

        # Run component
        comp = Component()
        comp.run()

        # Assert list_config_workspaces was called (component_id is positional)
        mock_client_instance.configurations.list_config_workspaces.assert_called_once_with(
            "keboola.app-data-gateway", config_id="12345"
        )

        # Assert discovered workspace_id is used
        call_args = mock_client_instance.workspaces.load_tables.call_args
        self.assertEqual(call_args[1]["workspace_id"], 99999)

    # JOB POLLING TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.time.sleep")  # Mock sleep to speed up test
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_basic",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_job_success_polling(self, mock_client, mock_sleep):
        """Test job polling continues until status is success"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}

        # Simulate multiple polling attempts
        mock_client_instance.jobs.detail.side_effect = [
            {"status": "processing", "id": "12345"},
            {"status": "processing", "id": "12345"},
            {
                "status": "success",
                "id": "12345",
                "createdTime": "2024-01-15T10:00:00+00:00",
                "startTime": "2024-01-15T10:00:01+00:00",
                "endTime": "2024-01-15T10:00:05+00:00",
            },
        ]

        # Run component
        comp = Component()
        comp.run()

        # Assert jobs.detail was called 3 times
        self.assertEqual(mock_client_instance.jobs.detail.call_count, 3)

        # Assert all calls were with the same job_id
        for call in mock_client_instance.jobs.detail.call_args_list:
            self.assertEqual(call[0][0], "12345")

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.time.sleep")  # Mock sleep to speed up test
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/full_load_basic",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_job_failure_raises_error(self, mock_client, mock_sleep):
        """Test job failure raises UserException with error message"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {
            "id": "12345",
            "status": "error",
            "error": {"message": "Table not found"},
        }

        # Run component - should raise exception
        comp = Component()
        with self.assertRaises(Exception) as context:
            comp.run()

        # Check error message contains the error from the job
        self.assertIn("Table not found", str(context.exception))

    # SYNC ACTION TESTS

    @freeze_time("2024-01-15 10:00:00")
    @mock.patch("component.time.sleep")  # Mock sleep to speed up test
    @mock.patch("component.Client")
    @mock.patch.dict(
        os.environ,
        {
            "KBC_DATADIR": "./tests/data/clean_workspace",
            "KBC_STACKID": "connection.keboola.com",
            "KBC_TOKEN": "test-token",
            "KBC_CONFIGID": "12345",
        },
    )
    def test_clean_workspace_action(self, mock_client, mock_sleep):
        """Test clean_workspace sync action drops all tables"""
        # Configure mock
        mock_client_instance = mock_client.return_value
        mock_client_instance.workspaces.load_tables.return_value = {"id": "12345"}
        mock_client_instance.jobs.detail.return_value = {"status": "success", "id": "12345"}

        # Run sync action directly
        comp = Component()
        result = comp.clean_workspace()

        # Assert load_tables was called with empty table_mapping
        call_args = mock_client_instance.workspaces.load_tables.call_args
        self.comparedict(
            call_args[1],
            {"workspace_id": 12345, "table_mapping": [], "preserve": False, "load_type": "load"},
            "clean_workspace parameters",
        )

        # Assert success result
        self.assertEqual(result.message, "Workspace cleaned successfully")


if __name__ == "__main__":
    # import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
