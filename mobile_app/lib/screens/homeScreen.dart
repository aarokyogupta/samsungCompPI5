import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../models/riskReport.dart';
import '../services/apiService.dart';
import '../services/websocketService.dart';
import '../widgets/alertBanner.dart';
import '../widgets/scoreCard.dart';
import 'reportDetailScreen.dart';

class HomeScreen extends StatefulWidget {
  const HomeScreen({super.key});
  @override
  State<HomeScreen> createState() => _HomeScreenState();
}

class _HomeScreenState extends State<HomeScreen> {
  List<RiskReport> reports = [];
  AlertEvent? activeAlert;
  bool loading = true;

  @override
  void initState() {
    super.initState();
    final service = context.read<ApiService>();
    service.getReports().then((items) => mounted ? setState(() { reports = items..sort((a, b) => b.criScore.compareTo(a.criScore)); loading = false; }) : null).catchError((_) { if (mounted) setState(() => loading = false); });
    context.read<WebsocketService>().events.listen((alert) { if (mounted) setState(() => activeAlert = alert); });
  }

  @override
  Widget build(BuildContext context) => Scaffold(
        appBar: AppBar(title: const Text('ICMIS Field Operations')),
        body: RefreshIndicator(
          onRefresh: () async => setState(() {}),
          child: ListView(padding: const EdgeInsets.all(16), children: [
            if (activeAlert != null) AlertBanner(alert: activeAlert!, onTap: () => setState(() => activeAlert = null)),
            const Text('Priority monitoring', style: TextStyle(fontSize: 20, fontWeight: FontWeight.bold)),
            const SizedBox(height: 12),
            if (loading) const Center(child: CircularProgressIndicator()),
            if (!loading && reports.isEmpty) const Card(child: Padding(padding: EdgeInsets.all(20), child: Text('No diagnostic reports are available offline.'))),
            for (final report in reports)
              GestureDetector(
                onTap: () => Navigator.of(context).push(MaterialPageRoute(builder: (_) => ReportDetailScreen(reportId: report.id))),
                child: ScoreCard(title: report.speciesName, score: report.criScore, subtitle: 'Species ${report.speciesId} · intervention level ${report.interventionLevel}'),
              ),
          ]),
        ),
      );
}
