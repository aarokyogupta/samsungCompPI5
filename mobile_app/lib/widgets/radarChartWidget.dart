import 'dart:math' as math;
import 'package:flutter/material.dart';

class RadarChartWidget extends StatelessWidget {
  const RadarChartWidget({super.key, required this.values});
  final Map<String, double> values;

  @override
  Widget build(BuildContext context) => SizedBox(height: 260, child: CustomPaint(painter: _RadarPainter(values, Theme.of(context).colorScheme)));
}

class _RadarPainter extends CustomPainter {
  _RadarPainter(this.values, this.scheme);
  final Map<String, double> values;
  final ColorScheme scheme;

  @override
  void paint(Canvas canvas, Size size) {
    final centre = Offset(size.width / 2, size.height / 2);
    final radius = math.min(size.width, size.height) / 2 - 30;
    final keys = ['population', 'habitat', 'threat', 'climate', 'genetics', 'behavior'];
    final grid = Paint()..style = PaintingStyle.stroke..color = scheme.outline.withOpacity(.5);
    final fill = Paint()..style = PaintingStyle.fill..color = scheme.primary.withOpacity(.25);
    final outline = Paint()..style = PaintingStyle.stroke..strokeWidth = 2..color = scheme.primary;
    Path polygon(double scale) {
      final path = Path();
      for (var index = 0; index < keys.length; index++) {
        final angle = -math.pi / 2 + index * 2 * math.pi / keys.length;
        final point = centre + Offset(math.cos(angle), math.sin(angle)) * radius * scale;
        index == 0 ? path.moveTo(point.dx, point.dy) : path.lineTo(point.dx, point.dy);
      }
      path.close();
      return path;
    }
    canvas.drawPath(polygon(1), grid);
    canvas.drawPath(polygon(.66), grid);
    final data = Path();
    for (var index = 0; index < keys.length; index++) {
      final angle = -math.pi / 2 + index * 2 * math.pi / keys.length;
      final scale = (values[keys[index]] ?? 0).clamp(0, 100) / 100;
      final point = centre + Offset(math.cos(angle), math.sin(angle)) * radius * scale;
      index == 0 ? data.moveTo(point.dx, point.dy) : data.lineTo(point.dx, point.dy);
    }
    data.close();
    canvas.drawPath(data, fill);
    canvas.drawPath(data, outline);
  }

  @override
  bool shouldRepaint(covariant _RadarPainter oldDelegate) => oldDelegate.values != values;
}
