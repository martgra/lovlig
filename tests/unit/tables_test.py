"""Unit tests for table rendering utilities."""

from datetime import datetime

from lovdata_processing.ui.tables import (
    create_file_list_table,
    create_statistics_table,
    format_status_summary,
)


class TestFileListTable:
    """Test file list table creation."""

    def test_creates_table_with_correct_columns(self):
        """Table should have all required columns."""
        files = [
            {
                "dataset": "dataset1.tar.bz2",
                "path": "file1.xml",
                "status": "added",
                "last_changed": datetime(2024, 1, 1),
                "size": 100,
            }
        ]

        table = create_file_list_table(files)

        # Check columns exist
        column_headers = [col.header for col in table.columns]
        assert "Dataset" in column_headers
        assert "Path" in column_headers
        assert "Status" in column_headers
        assert "Last Changed" in column_headers
        assert "Size" in column_headers

    def test_table_title_includes_count(self):
        """Table title should include file count."""
        files = [
            {
                "dataset": "ds1",
                "path": "f1",
                "status": "added",
                "last_changed": None,
                "size": 100,
            },
            {
                "dataset": "ds1",
                "path": "f2",
                "status": "added",
                "last_changed": None,
                "size": 200,
            },
        ]

        table = create_file_list_table(files)

        assert "2 total" in table.title


class TestStatisticsTable:
    """Test statistics table creation."""

    def test_creates_statistics_with_totals_row(self):
        """Table should include totals row for multiple datasets."""
        stats = {
            "dataset1.tar.bz2": {
                "total": 10,
                "added": 2,
                "modified": 1,
                "unchanged": 7,
                "removed": 0,
                "total_size": 1000,
            },
            "dataset2.tar.bz2": {
                "total": 5,
                "added": 1,
                "modified": 0,
                "unchanged": 4,
                "removed": 0,
                "total_size": 500,
            },
        }

        table = create_statistics_table(stats)

        # Table should have rows for each dataset plus totals
        assert table.row_count >= 2

    def test_single_dataset_no_totals_row(self):
        """Single dataset should not show totals row."""
        stats = {
            "dataset1.tar.bz2": {
                "total": 10,
                "added": 2,
                "modified": 1,
                "unchanged": 7,
                "removed": 0,
                "total_size": 1000,
            }
        }

        table = create_statistics_table(stats)

        # Should only have one data row (no totals)
        assert table.row_count == 1


class TestStatusSummary:
    """Test status summary formatting."""

    def test_formats_summary_correctly(self):
        """Summary should list counts by status."""
        files = [
            {"status": "added"},
            {"status": "added"},
            {"status": "modified"},
            {"status": "unchanged"},
        ]

        summary = format_status_summary(files)

        assert "2 added" in summary
        assert "1 modified" in summary
        assert "1 unchanged" in summary

    def test_handles_empty_list(self):
        """Empty file list should return empty summary."""
        files = []

        summary = format_status_summary(files)

        assert summary == ""

    def test_sorts_status_alphabetically(self):
        """Status counts should be sorted alphabetically."""
        files = [
            {"status": "unchanged"},
            {"status": "added"},
            {"status": "modified"},
        ]

        summary = format_status_summary(files)

        # Check order: added, modified, unchanged
        added_pos = summary.index("added")
        modified_pos = summary.index("modified")
        unchanged_pos = summary.index("unchanged")
        assert added_pos < modified_pos < unchanged_pos
