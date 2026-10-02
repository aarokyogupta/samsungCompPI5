import 'package:fl_chart/fl_chart.dart';
import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../services/apiService.dart';
import '../widgets/radarChartWidget.dart';

class AnalyticsScreen extends StatefulWidget {
  const AnalyticsScreen({super.key});
  @override
  State<AnalyticsScreen> createState() => _AnalyticsScreenState();
}

class _AnalyticsScreenState extends State<AnalyticsScreen> {
  Map<String, dynamic>? historical;
  @override
  void initState() { super.initState(); context.read<ApiService>().getHistorical().then((value) { if (mounted) setState(() => historical = value); }).catchError((_) {}); }

  @override
  Widget build(BuildContext context) {
    final values = <String, double>{for (final name in ['population', 'habitat', 'threat', 'climate', 'genetics', 'behavior']) name: 50};
    final datasets = historical?['datasets'] as List?;
    final data = datasets != null && datasets.isNotEmpty ? (datasets.first as Map).cast<String, dynamic>() : null;
    final points = ((data?['data'] as List?) ?? []).whereType<num>().toList();
    return Scaffold(appBar: AppBar(title: const Text('Analytics')), body: ListView(padding: const EdgeInsets.all(16), children: [
      const Text('Critical Risk Index', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
      SizedBox(height: 220, child: LineChart(LineChartData(lineBarsData: [LineChartBarData(isCurved: true, spots: [for (var i = 0; i < points.length; i++) FlSpot(i.toDouble(), points[i].toDouble())])]))),
      const Text('Ecological subscores', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
      RadarChartWidget(values: values),
    ]));
  }
}
