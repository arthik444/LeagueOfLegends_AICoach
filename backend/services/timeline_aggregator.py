import json
from pathlib import Path
from typing import Dict, List
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class TimelineAggregator:
    """
    Service to aggregate timeline data for year recap heatmaps.
    Reads timeline data dynamically from the Sneaky_data directory.
    """

    def __init__(self, data_dir: str = "Sneaky_data"):
        self.data_dir = Path(data_dir)
        self.timelines_dir = self.data_dir / "matches" / "timelines"
        self.matches_dir = self.data_dir / "matches"

    def _get_participant_id_for_puuid(self, match_id: str, target_puuid: str) -> int:
        """Get the participant ID for a given PUUID in a match"""
        # Try to find the match file
        match_files = list(self.matches_dir.glob(f"match_*{match_id}.json"))

        if not match_files:
            logger.warning(f"Match file not found for {match_id}")
            return None

        try:
            with open(match_files[0], 'r') as f:
                match_data = json.load(f)

            participants = match_data['metadata']['participants']
            for idx, puuid in enumerate(participants, 1):
                if puuid == target_puuid:
                    return idx

        except Exception as e:
            logger.error(f"Error reading match file {match_files[0]}: {e}")

        return None

    def generate_heatmap_data(self, target_puuid: str, player_name: str = "Player") -> Dict:
        """
        Generate heatmap data for all timeline events for a specific player.

        Returns:
            Dict with stats and heatmap data for deaths, kills, assists, objectives
        """
        logger.info(f"Generating heatmap data for {player_name}")

        # Get all timeline files
        timeline_files = list(self.timelines_dir.glob("timeline_*.json"))
        if not timeline_files:
            logger.warning("No timeline files found")
            return self._empty_response(target_puuid, player_name)

        # Build match_id -> participant_id mapping
        puuid_to_participant_map = {}

        for timeline_file in timeline_files:
            if timeline_file.name == "fetch_summary.json":
                continue

            try:
                with open(timeline_file, 'r') as f:
                    timeline_data = json.load(f)

                match_id = timeline_data['metadata']['matchId']
                participant_id = self._get_participant_id_for_puuid(match_id, target_puuid)

                if participant_id:
                    puuid_to_participant_map[match_id] = participant_id

            except Exception as e:
                logger.error(f"Error reading {timeline_file.name}: {e}")
                continue

        logger.info(f"Found player in {len(puuid_to_participant_map)} matches")

        # Initialize data structures
        heatmap_data = {
            "deaths": [],
            "kills": [],
            "assists": [],
            "objectives": []
        }

        stats = {
            "total_matches": len(puuid_to_participant_map),
            "deaths_count": 0,
            "kills_count": 0,
            "assists_count": 0,
            "objectives_count": 0
        }

        # Timeline statistics - track events by time (in minutes)
        # Each bucket represents a 1-minute interval
        timeline_stats = {
            "deaths": defaultdict(int),
            "kills": defaultdict(int),
            "assists": defaultdict(int),
            "objectives": defaultdict(int)
        }

        # Process each timeline file
        for timeline_file in timeline_files:
            if timeline_file.name == "fetch_summary.json":
                continue

            try:
                with open(timeline_file, 'r') as f:
                    timeline_data = json.load(f)

                match_id = timeline_data['metadata']['matchId']

                if match_id not in puuid_to_participant_map:
                    continue

                player_participant_id = puuid_to_participant_map[match_id]

                # Process each frame
                for frame in timeline_data['info']['frames']:
                    # Process events
                    for event in frame.get('events', []):
                        if not event.get('position'):
                            continue

                        pos = event['position']
                        timestamp = event['timestamp']

                        # Calculate minute bucket for timeline
                        minute_bucket = timestamp // 60000  # Convert ms to minutes

                        # DEATHS: Player was killed
                        if event['type'] == 'CHAMPION_KILL' and event.get('victimId') == player_participant_id:
                            heatmap_data['deaths'].append({
                                'x': pos['x'],
                                'y': pos['y'],
                                'timestamp': timestamp,
                                'match_id': match_id,
                                'killer_id': event.get('killerId')
                            })
                            stats['deaths_count'] += 1
                            timeline_stats['deaths'][minute_bucket] += 1

                        # KILLS: Player got the kill
                        elif event['type'] == 'CHAMPION_KILL' and event.get('killerId') == player_participant_id:
                            heatmap_data['kills'].append({
                                'x': pos['x'],
                                'y': pos['y'],
                                'timestamp': timestamp,
                                'match_id': match_id,
                                'victim_id': event.get('victimId')
                            })
                            stats['kills_count'] += 1
                            timeline_stats['kills'][minute_bucket] += 1

                        # ASSISTS: Player got an assist
                        elif event['type'] == 'CHAMPION_KILL' and player_participant_id in event.get('assistingParticipantIds', []):
                            heatmap_data['assists'].append({
                                'x': pos['x'],
                                'y': pos['y'],
                                'timestamp': timestamp,
                                'match_id': match_id,
                                'victim_id': event.get('victimId')
                            })
                            stats['assists_count'] += 1
                            timeline_stats['assists'][minute_bucket] += 1

                        # OBJECTIVES: Player participated in objective kills
                        elif event['type'] == 'ELITE_MONSTER_KILL' and event.get('killerId') == player_participant_id:
                            heatmap_data['objectives'].append({
                                'x': pos['x'],
                                'y': pos['y'],
                                'timestamp': timestamp,
                                'match_id': match_id,
                                'monster_type': event.get('monsterType')
                            })
                            stats['objectives_count'] += 1
                            timeline_stats['objectives'][minute_bucket] += 1

                        elif event['type'] == 'BUILDING_KILL':
                            # Check if player was involved
                            assisting = event.get('assistingParticipantIds', [])
                            if player_participant_id in assisting or event.get('killerId') == player_participant_id:
                                heatmap_data['objectives'].append({
                                    'x': pos['x'],
                                    'y': pos['y'],
                                    'timestamp': timestamp,
                                    'match_id': match_id,
                                    'building_type': event.get('buildingType')
                                })
                                stats['objectives_count'] += 1
                                timeline_stats['objectives'][minute_bucket] += 1

            except Exception as e:
                logger.error(f"Error processing {timeline_file.name}: {e}")
                continue

        logger.info(f"Generated heatmap: {stats}")

        # Convert timeline stats to arrays for easier frontend consumption
        # Calculate cumulative and per-minute averages
        total_matches = len(puuid_to_participant_map) or 1  # Avoid division by zero

        timeline_data = {
            "deaths": self._format_timeline_data(timeline_stats['deaths'], total_matches),
            "kills": self._format_timeline_data(timeline_stats['kills'], total_matches),
            "assists": self._format_timeline_data(timeline_stats['assists'], total_matches),
            "objectives": self._format_timeline_data(timeline_stats['objectives'], total_matches)
        }

        return {
            "player_puuid": target_puuid,
            "player_name": player_name,
            "stats": stats,
            "heatmap_data": heatmap_data,
            "timeline_data": timeline_data
        }

    def _format_timeline_data(self, minute_buckets: Dict[int, int], total_matches: int) -> List[Dict]:
        """
        Format timeline data for frontend consumption.
        Returns array of {minute, count, cumulative, average_per_game}
        """
        if not minute_buckets:
            return []

        # Get max minute to establish range
        max_minute = max(minute_buckets.keys()) if minute_buckets else 0

        result = []
        cumulative = 0

        for minute in range(0, max_minute + 1):
            count = minute_buckets.get(minute, 0)
            cumulative += count

            result.append({
                "minute": minute,
                "count": count,  # Total across all matches
                "cumulative": cumulative,  # Cumulative total
                "average_per_game": round(count / total_matches, 2)  # Average per game
            })

        return result

    def _empty_response(self, puuid: str, player_name: str) -> Dict:
        """Return empty response structure"""
        return {
            "player_puuid": puuid,
            "player_name": player_name,
            "stats": {
                "total_matches": 0,
                "deaths_count": 0,
                "kills_count": 0,
                "assists_count": 0,
                "objectives_count": 0
            },
            "heatmap_data": {
                "deaths": [],
                "kills": [],
                "assists": [],
                "objectives": []
            },
            "timeline_data": {
                "deaths": [],
                "kills": [],
                "assists": [],
                "objectives": []
            }
        }
