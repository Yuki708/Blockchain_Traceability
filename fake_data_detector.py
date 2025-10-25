# fake_data_detector_final.py
# Copyright 2024 Hyperledger Sawtooth Data Trace Experiment
# Licensed under Apache License 2.0

import logging
from collections import Counter
import json

logger = logging.getLogger(__name__)

class FakeDataDetector:
    def detect_fake_data(self, original_dataset, test_data, detection_method='statistical'):
        if detection_method == 'statistical':
            return self.statistical_detection(original_dataset, test_data)
        elif detection_method == 'pattern_based':
            return self.pattern_based_detection(original_dataset, test_data)
        elif detection_method == 'frequency_based':
            return self.frequency_based_detection(original_dataset, test_data)
        else:
            raise ValueError(f"Unsupported detection method: {detection_method}")

    def statistical_detection(self, original, test):
        original_stats = self._extract_statistical_features(original)
        test_stats = self._extract_statistical_features(test)

        count_similarity = self._calculate_similarity(original_stats['command_count'], test_stats['command_count'])
        unique_similarity = self._calculate_similarity(original_stats['unique_commands'], test_stats['unique_commands'])
        distribution_similarity = self._calculate_distribution_similarity(original_stats['command_frequency'], test_stats['command_frequency'])

        overall_similarity = (count_similarity + unique_similarity + distribution_similarity) / 3
        is_fake = overall_similarity < 0.7
        confidence = 1.0 - overall_similarity if is_fake else overall_similarity

        return {
            'is_fake': is_fake,
            'confidence': confidence,
            'method': 'statistical',
            'similarity_scores': {
                'count_similarity': count_similarity,
                'unique_similarity': unique_similarity,
                'distribution_similarity': distribution_similarity,
                'overall_similarity': overall_similarity
            }
        }

    def pattern_based_detection(self, original, test):
        original_patterns = self._extract_patterns(original.get('commands', []))
        test_patterns = self._extract_patterns(test.get('commands', []))
        pattern_overlap = self._calculate_pattern_overlap(original_patterns, test_patterns)

        is_fake = pattern_overlap < 0.5
        confidence = 1.0 - pattern_overlap if is_fake else pattern_overlap

        return {
            'is_fake': is_fake,
            'confidence': confidence,
            'method': 'pattern_based',
            'pattern_overlap': pattern_overlap
        }

    def frequency_based_detection(self, original, test):
        original_freq = Counter(original.get('commands', []))
        test_freq = Counter(test.get('commands', []))
        freq_distance = self._calculate_frequency_distance(original_freq, test_freq)

        max_distance = len(original_freq) + len(test_freq)
        normalized_distance = freq_distance / max_distance if max_distance > 0 else 0

        is_fake = normalized_distance > 0.6
        confidence = normalized_distance if is_fake else 1.0 - normalized_distance

        return {
            'is_fake': is_fake,
            'confidence': confidence,
            'method': 'frequency_based',
            'frequency_distance': freq_distance,
            'normalized_distance': normalized_distance
        }

    def _extract_statistical_features(self, dataset):
        commands = dataset.get('commands', [])
        command_counts = Counter(commands)
        return {
            'command_count': len(commands),
            'unique_commands': len(set(commands)),
            'command_frequency': dict(command_counts)
        }

    def _calculate_similarity(self, val1, val2):
        if val1 == 0 and val2 == 0:
            return 1.0
        if val1 == 0 or val2 == 0:
            return 0.0
        diff = abs(val1 - val2)
        max_val = max(val1, val2)
        return 1.0 - (diff / max_val)

    def _calculate_distribution_similarity(self, freq1, freq2):
        all_commands = set(freq1.keys()) | set(freq2.keys())
        p1 = [freq1.get(cmd, 0) for cmd in all_commands]
        p2 = [freq2.get(cmd, 0) for cmd in all_commands]
        total1 = sum(p1)
        total2 = sum(p2)
        if total1 == 0 or total2 == 0:
            return 0.0
        p1_norm = [x / total1 for x in p1]
        p2_norm = [x / total2 for x in p2]
        jsd = self._jensen_shannon_divergence(p1_norm, p2_norm)
        return 1.0 - jsd

    def _jensen_shannon_divergence(self, p, q):
        import math
        m = [(pi + qi) / 2 for pi, qi in zip(p, q)]
        return (self._kl_divergence(p, m) + self._kl_divergence(q, m)) / 2

    def _kl_divergence(self, p, q):
        import math
        return sum(pi * math.log(pi / qi) if pi > 0 and qi > 0 else 0 for pi, qi in zip(p, q))

    def _extract_patterns(self, commands, pattern_length=3):
        patterns = Counter()
        for i in range(len(commands) - pattern_length + 1):
            pattern = tuple(commands[i:i+pattern_length])
            patterns[pattern] += 1
        return patterns

    def _calculate_pattern_overlap(self, patterns1, patterns2):
        top1 = set(patterns1.most_common(10))
        top2 = set(patterns2.most_common(10))
        intersection = top1 & top2
        union = top1 | top2
        return len(intersection) / len(union) if union else 0.0

    def _calculate_frequency_distance(self, freq1, freq2):
        all_commands = set(freq1.keys()) | set(freq2.keys())
        distance = 0
        for cmd in all_commands:
            count1 = freq1.get(cmd, 0)
            count2 = freq2.get(cmd, 0)
            distance += abs(count1 - count2)
        return distance
