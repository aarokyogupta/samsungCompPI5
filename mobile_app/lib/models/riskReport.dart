class RiskReport {
  const RiskReport({
    required this.id,
    required this.speciesId,
    required this.criScore,
    required this.subscores,
    this.speciesName = 'Unknown species',
    this.narrative = '',
    this.effectivePopulationSize,
    this.expectedHeterozygosity,
    this.interventionLevel = 1,
    this.causalFactors = const [],
    this.checklistItems = const [],
  });

  final String id;
  final String speciesId;
  final String speciesName;
  final double criScore;
  final String narrative;
  final double? effectivePopulationSize;
  final double? expectedHeterozygosity;
  final Map<String, double> subscores;
  final int interventionLevel;
  final List<String> causalFactors;
  final List<ChecklistItem> checklistItems;

  factory RiskReport.fromJson(Map<String, dynamic> json) {
    final scores = (json['scores'] as Map?)?.cast<String, dynamic>() ?? {};
    final rawSubscores = (scores['subscores'] as Map?)?.cast<String, dynamic>() ?? {};
    return RiskReport(
      id: '${json['id'] ?? json['report_id'] ?? ''}',
      speciesId: '${json['species_id'] ?? json['species']?['id'] ?? ''}',
      speciesName: '${json['species']?['common_name'] ?? json['species']?['name'] ?? 'Unknown species'}',
      criScore: _number(scores['cri_score'] ?? json['cri_score']),
      narrative: '${json['narrative_report'] ?? json['narrative'] ?? ''}',
      effectivePopulationSize: _optional(scores['effective_population_size'] ?? json['effective_population_size']),
      expectedHeterozygosity: _optional(scores['expected_heterozygosity'] ?? json['expected_heterozygosity']),
      subscores: {
        for (final entry in rawSubscores.entries) entry.key: _number(entry.value),
      },
      interventionLevel: (json['intervention_level'] as num?)?.toInt() ?? 1,
      causalFactors: (json['causal_factors'] as List?)?.map((item) => '$item').toList() ?? const [],
      checklistItems: (json['checklist_items'] as List?)
              ?.whereType<Map>()
              .map((item) => ChecklistItem(label: '${item['label'] ?? item['task'] ?? ''}', completed: item['is_completed'] == true))
              .toList() ??
          const [],
    );
  }

  static double _number(dynamic value) => (value as num?)?.toDouble() ?? 0;
  static double? _optional(dynamic value) => (value as num?)?.toDouble();
}

class ChecklistItem {
  const ChecklistItem({required this.label, this.completed = false});
  final String label;
  final bool completed;
}
