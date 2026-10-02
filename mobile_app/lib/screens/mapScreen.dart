import 'package:flutter/material.dart';
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';
import 'package:provider/provider.dart';
import '../models/telemetryPoint.dart';
import '../services/apiService.dart';

class MapScreen extends StatefulWidget {
  const MapScreen({super.key});
  @override
  State<MapScreen> createState() => _MapScreenState();
}

class _MapScreenState extends State<MapScreen> {
  List<TelemetryPoint> points = [];
  @override
  void initState() {
    super.initState();
    context.read<ApiService>().getTelemetry().then((value) { if (mounted) setState(() => points = value); }).catchError((_) {});
  }

  @override
  Widget build(BuildContext context) => Scaffold(
        appBar: AppBar(title: const Text('Telemetry map')),
        body: FlutterMap(
          options: const MapOptions(initialCenter: LatLng(-2.334, 34.821), initialZoom: 7),
          children: [
            TileLayer(urlTemplate: 'https://tile.openstreetmap.org/{z}/{x}/{y}.png', userAgentPackageName: 'org.icmis.mobile'),
            PolylineLayer(polylines: [Polyline(points: points.map((point) => LatLng(point.latitude, point.longitude)).toList(), color: Colors.cyanAccent, strokeWidth: 4)]),
            MarkerLayer(markers: points.map((point) => Marker(point: LatLng(point.latitude, point.longitude), width: 36, height: 36, child: const Icon(Icons.location_on, color: Colors.orange))).toList()),
          ],
        ),
      );
}
