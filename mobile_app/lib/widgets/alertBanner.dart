import 'package:flutter/material.dart';
import '../services/websocketService.dart';

class AlertBanner extends StatelessWidget {
  const AlertBanner({super.key, required this.alert, this.onTap});
  final AlertEvent alert;
  final VoidCallback? onTap;

  @override
  Widget build(BuildContext context) => Card(
        color: alert.criScore > 80 || alert.priority == 'CRITICAL' ? Colors.red.shade900 : Colors.orange.shade900,
        child: ListTile(
          onTap: onTap,
          leading: const Icon(Icons.warning_amber_rounded),
          title: Text('${alert.priority} · ${alert.classification}'),
          subtitle: Text(alert.criScore > 0 ? 'CRI ${alert.criScore.toStringAsFixed(0)} · Tap for details' : 'Tap for details'),
        ),
      );
}
