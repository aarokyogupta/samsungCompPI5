import 'package:flutter/material.dart';

class ScoreCard extends StatelessWidget {
  const ScoreCard({super.key, required this.title, required this.score, this.subtitle});
  final String title;
  final double score;
  final String? subtitle;

  @override
  Widget build(BuildContext context) {
    final colour = score > 80 ? Colors.redAccent : score >= 65 ? Colors.orangeAccent : score >= 50 ? Colors.amber : Colors.greenAccent;
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(16),
        child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
          Text(title, style: Theme.of(context).textTheme.labelLarge),
          const SizedBox(height: 8),
          Text(score.toStringAsFixed(0), style: Theme.of(context).textTheme.headlineMedium?.copyWith(color: colour, fontWeight: FontWeight.bold)),
          if (subtitle != null) Text(subtitle!, style: Theme.of(context).textTheme.bodySmall),
        ]),
      ),
    );
  }
}
