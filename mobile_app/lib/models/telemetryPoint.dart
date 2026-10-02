class TelemetryPoint {
  const TelemetryPoint({
    required this.sensorId,
    required this.latitude,
    required this.longitude,
    required this.timestamp,
    this.heading,
    this.speed,
    this.signalStrength,
  });

  final String sensorId;
  final double latitude;
  final double longitude;
  final DateTime timestamp;
  final double? heading;
  final double? speed;
  final int? signalStrength;

  factory TelemetryPoint.fromJson(Map<String, dynamic> json) {
    return TelemetryPoint(
      sensorId: '${json['sensor_id'] ?? json['sensorId'] ?? 'unknown'}',
      latitude: _number(json['latitude'] ?? json['lat']),
      longitude: _number(json['longitude'] ?? json['lng']),
      timestamp: DateTime.tryParse('${json['recorded_at'] ?? json['timestamp'] ?? ''}')?.toUtc() ?? DateTime.now().toUtc(),
      heading: _optionalNumber(json['heading']),
      speed: _optionalNumber(json['speed']),
      signalStrength: (json['signal_strength'] ?? json['signalStrength'] as num?)?.toInt(),
    );
  }

  static double _number(dynamic value) => (value as num?)?.toDouble() ?? 0;
  static double? _optionalNumber(dynamic value) => (value as num?)?.toDouble();
}
