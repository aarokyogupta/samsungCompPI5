import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../models/riskReport.dart';
import '../services/apiService.dart';

class ReportDetailScreen extends StatefulWidget {
  const ReportDetailScreen({super.key, required this.reportId});
  final String reportId;
  @override
  State<ReportDetailScreen> createState() => _ReportDetailScreenState();
}

class _ReportDetailScreenState extends State<ReportDetailScreen> {
  RiskReport? report;
  final completed = <int, bool>{};
  @override
  void initState() { super.initState(); context.read<ApiService>().getReport(widget.reportId).then((value) { if (mounted) setState(() => report = value); }).catchError((_) {}); }

  @override
  Widget build(BuildContext context) {
    final item = report;
    return Scaffold(appBar: AppBar(title: const Text('Risk diagnostic')), body: item == null ? const Center(child: CircularProgressIndicator()) : ListView(padding: const EdgeInsets.all(16), children: [
      Text(item.speciesName, style: Theme.of(context).textTheme.headlineSmall),
      Text('CRI ${item.criScore.toStringAsFixed(0)} · Intervention level ${item.interventionLevel}'),
      const SizedBox(height: 16),
      Text(item.narrative.isEmpty ? 'No narrative is available.' : item.narrative),
      const SizedBox(height: 16),
      for (var index = 0; index < item.checklistItems.length; index++)
        CheckboxListTile(
          value: completed[index] ?? item.checklistItems[index].completed,
          title: Text(item.checklistItems[index].label),
          onChanged: (value) { setState(() => completed[index] = value ?? false); context.read<ApiService>().updateChecklist(item.id, index, value ?? false); },
        ),
    ]));
  }
}
