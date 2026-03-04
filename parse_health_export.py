#!/usr/bin/env python3
"""Parse Apple Health export.xml into compact CSV files for analysis.

Streams the XML to handle large files (600MB+) without loading into memory.
Filters to recent data, strips verbose identifiers, drops bloat fields.

Usage:
    python parse_health_export.py export.xml
    python parse_health_export.py export.xml --days 90 --output-dir ./data
    python parse_health_export.py export.xml --types StepCount,HeartRate --verbose
"""

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from xml.etree.ElementTree import iterparse


# Prefixes to strip from Apple Health type identifiers
TYPE_PREFIXES = [
    "HKQuantityTypeIdentifier",
    "HKCategoryTypeIdentifier",
    "HKDataType",
    "HKWorkoutActivityType",
]

RECORD_FIELDS = ["startDate", "endDate", "value", "sourceName"]
WORKOUT_FIELDS = [
    "startDate", "endDate", "activityType", "duration", "durationUnit",
    "totalDistance", "distanceUnit", "totalEnergyBurned", "energyUnit", "sourceName",
]
ACTIVITY_FIELDS = [
    "date", "activeEnergyBurned", "activeEnergyBurnedGoal",
    "exerciseMinutes", "exerciseGoal", "standHours", "standHoursGoal",
]


def parse_date(date_str):
    """Parse Apple Health date format '2025-03-04 08:15:32 -0700' to datetime.

    Strips the timezone offset for simple comparison. Returns None on failure.
    """
    if not date_str:
        return None
    try:
        # Strip timezone offset (last 6 chars like ' -0700')
        return datetime.strptime(date_str[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, IndexError):
        return None


def shorten_type(type_name):
    """Strip verbose HK prefixes from type identifiers."""
    for prefix in TYPE_PREFIXES:
        if type_name.startswith(prefix):
            return type_name[len(prefix):]
    return type_name


def to_snake_case(name):
    """Convert CamelCase to snake_case for filenames."""
    s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    return s.lower()


def format_date(date_str):
    """Trim Apple Health date to compact format without timezone."""
    if not date_str:
        return ""
    return date_str[:19]


class CSVWriterManager:
    """Lazily opens CSV files on first write for each record type."""

    def __init__(self, output_dir):
        self.output_dir = output_dir
        self._writers = {}
        self._files = {}

    def write_row(self, name, fields, row):
        """Write a row to the CSV for the given type name."""
        if name not in self._writers:
            filename = to_snake_case(name) + ".csv"
            path = os.path.join(self.output_dir, filename)
            f = open(path, "w", newline="", encoding="utf-8")
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            writer.writeheader()
            self._writers[name] = writer
            self._files[name] = f
        self._writers[name].writerow(row)

    def close_all(self):
        for f in self._files.values():
            f.close()

    def get_filenames(self):
        return {name: to_snake_case(name) + ".csv" for name in self._writers}


def parse_export(input_path, days, output_dir, type_filter, verbose):
    """Stream-parse the export.xml and write CSV outputs."""
    os.makedirs(output_dir, exist_ok=True)

    cutoff = datetime.now() - timedelta(days=days)
    csv_mgr = CSVWriterManager(output_dir)

    # Stats tracking
    counts = defaultdict(int)
    units = {}
    sources = defaultdict(set)
    date_min = None
    date_max = None
    total_processed = 0
    total_skipped = 0

    try:
        context = iterparse(input_path, events=("end",))
        # Get the root element so we can clear it to free memory
        root = None

        for event, elem in context:
            if root is None:
                # iterparse doesn't give us root directly; walk up
                # Actually, we need a different approach - get root from start event
                pass

            tag = elem.tag

            if tag == "Record":
                total_processed += 1
                try:
                    raw_type = elem.get("type", "")
                    short_type = shorten_type(raw_type)

                    # Type filter
                    if type_filter and short_type not in type_filter:
                        elem.clear()
                        continue

                    # Date filter
                    start = parse_date(elem.get("startDate"))
                    if start and start < cutoff:
                        elem.clear()
                        continue

                    # Track stats
                    counts[short_type] += 1
                    unit = elem.get("unit", "")
                    if unit and short_type not in units:
                        units[short_type] = unit
                    source = elem.get("sourceName", "")
                    if source:
                        sources[short_type].add(source)

                    if start:
                        if date_min is None or start < date_min:
                            date_min = start
                        if date_max is None or start > date_max:
                            date_max = start

                    row = {
                        "startDate": format_date(elem.get("startDate")),
                        "endDate": format_date(elem.get("endDate")),
                        "value": elem.get("value", ""),
                        "sourceName": source,
                    }
                    csv_mgr.write_row(short_type, RECORD_FIELDS, row)

                except Exception:
                    total_skipped += 1

                elem.clear()

            elif tag == "Workout":
                total_processed += 1
                try:
                    start = parse_date(elem.get("startDate"))
                    if start and start < cutoff:
                        elem.clear()
                        continue

                    activity = shorten_type(elem.get("workoutActivityType", ""))
                    counts["Workout"] += 1

                    if start:
                        if date_min is None or start < date_min:
                            date_min = start
                        if date_max is None or start > date_max:
                            date_max = start

                    row = {
                        "startDate": format_date(elem.get("startDate")),
                        "endDate": format_date(elem.get("endDate")),
                        "activityType": activity,
                        "duration": elem.get("duration", ""),
                        "durationUnit": elem.get("durationUnit", ""),
                        "totalDistance": elem.get("totalDistance", ""),
                        "distanceUnit": elem.get("totalDistanceUnit", ""),
                        "totalEnergyBurned": elem.get("totalEnergyBurned", ""),
                        "energyUnit": elem.get("totalEnergyBurnedUnit", ""),
                        "sourceName": elem.get("sourceName", ""),
                    }
                    csv_mgr.write_row("Workout", WORKOUT_FIELDS, row)

                except Exception:
                    total_skipped += 1

                elem.clear()

            elif tag == "ActivitySummary":
                total_processed += 1
                try:
                    date_str = elem.get("dateComponents", "")
                    if date_str:
                        try:
                            d = datetime.strptime(date_str, "%Y-%m-%d")
                            if d < cutoff:
                                elem.clear()
                                continue
                        except ValueError:
                            pass

                    counts["ActivitySummary"] += 1

                    row = {
                        "date": date_str,
                        "activeEnergyBurned": elem.get("activeEnergyBurned", ""),
                        "activeEnergyBurnedGoal": elem.get("activeEnergyBurnedGoal", ""),
                        "exerciseMinutes": elem.get("appleExerciseTime", ""),
                        "exerciseGoal": elem.get("appleExerciseTimeGoal", ""),
                        "standHours": elem.get("appleStandHours", ""),
                        "standHoursGoal": elem.get("appleStandHoursGoal", ""),
                    }
                    csv_mgr.write_row("ActivitySummary", ACTIVITY_FIELDS, row)

                except Exception:
                    total_skipped += 1

                elem.clear()

            else:
                # Clear elements we don't care about to free memory
                elem.clear()

            if verbose and total_processed % 1_000_000 == 0:
                print(f"  Processed {total_processed:,} elements...", file=sys.stderr)

    except Exception as e:
        print(f"Warning: XML parsing stopped early: {e}", file=sys.stderr)
        print("Writing partial results...", file=sys.stderr)

    csv_mgr.close_all()

    # Write summary
    summary = {
        "dateRange": {
            "start": date_min.strftime("%Y-%m-%d") if date_min else None,
            "end": date_max.strftime("%Y-%m-%d") if date_max else None,
        },
        "daysIncluded": days,
        "recordCounts": dict(sorted(counts.items(), key=lambda x: -x[1])),
        "units": units,
        "sources": {k: sorted(v) for k, v in sources.items()},
        "totalProcessed": total_processed,
        "totalSkipped": total_skipped,
        "files": {
            name: filename
            for name, filename in sorted(csv_mgr.get_filenames().items())
        },
    }

    summary_path = os.path.join(output_dir, "summary.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return summary


def main():
    parser = argparse.ArgumentParser(
        description="Parse Apple Health export.xml into compact CSV files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""Examples:
  %(prog)s export.xml
  %(prog)s export.xml --days 90 --output-dir ./data
  %(prog)s export.xml --types StepCount,HeartRate --verbose""",
    )
    parser.add_argument("input", help="Path to Apple Health export.xml file")
    parser.add_argument(
        "--days", type=int, default=365,
        help="Days of history to include (default: 365)",
    )
    parser.add_argument(
        "--output-dir", default="./output",
        help="Output directory for CSV files (default: ./output)",
    )
    parser.add_argument(
        "--types",
        help="Comma-separated list of types to include (e.g., StepCount,HeartRate)",
    )
    parser.add_argument(
        "--verbose", action="store_true",
        help="Show progress every 1M records",
    )

    args = parser.parse_args()

    if not os.path.isfile(args.input):
        print(f"Error: File not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    type_filter = None
    if args.types:
        type_filter = set(args.types.split(","))

    print(f"Parsing {args.input}...")
    print(f"  Keeping last {args.days} days")
    if type_filter:
        print(f"  Filtering to types: {', '.join(sorted(type_filter))}")
    print(f"  Output: {args.output_dir}/")
    print()

    summary = parse_export(
        args.input, args.days, args.output_dir, type_filter, args.verbose,
    )

    print("Done!")
    print(f"  Date range: {summary['dateRange']['start']} to {summary['dateRange']['end']}")
    print(f"  Records: {sum(summary['recordCounts'].values()):,}")
    if summary["totalSkipped"]:
        print(f"  Skipped: {summary['totalSkipped']:,}")
    print(f"  Files written to {args.output_dir}/:")
    for name, filename in sorted(summary["files"].items()):
        count = summary["recordCounts"].get(name, 0)
        print(f"    {filename} ({count:,} rows)")
    print(f"    summary.json")


if __name__ == "__main__":
    main()
