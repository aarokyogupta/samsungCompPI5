import 'dart:async';
import 'dart:convert';

import 'package:dio/dio.dart';
import 'package:hive_flutter/hive_flutter.dart';

import '../models/riskReport.dart';
import '../models/telemetryPoint.dart';

// Build-time defaults; the Settings tab overrides both and remembers them in Hive
const String defaultApiUrl = String.fromEnvironment('ICMIS_API_URL', defaultValue: 'http://icmis.local:8000/api/v1');
const String defaultApiKey = String.fromEnvironment('ICMIS_API_KEY');

class ApiService {
  ApiService({String? baseUrl, String? apiKey})
      : _baseUrl = baseUrl ?? defaultApiUrl,
        _apiKey = apiKey ?? defaultApiKey,
        _dio = Dio(BaseOptions(
          baseUrl: baseUrl ?? defaultApiUrl,
          headers: {if ((apiKey ?? defaultApiKey).isNotEmpty) 'X-API-Key': apiKey ?? defaultApiKey},
          connectTimeout: const Duration(seconds: 8),
          receiveTimeout: const Duration(seconds: 15),
        ));

  final Dio _dio;
  final List<Map<String, dynamic>> _pendingWrites = [];
  late Box<String> _cache;
  late Box<String> _queue;
  late Box<String> _connection;
  String _baseUrl;
  String _apiKey;

  String get baseUrl => _baseUrl;
  String get apiKey => _apiKey;

  Future<void> initialise() async {
    await Hive.initFlutter();
    _cache = await Hive.openBox<String>('icmis_cache');
    _queue = await Hive.openBox<String>('icmis_request_queue');
    _connection = await Hive.openBox<String>('icmis_connection');
    _pendingWrites.addAll(_queue.values.map((item) => jsonDecode(item) as Map<String, dynamic>));
    // A phone paired with the Pi once keeps talking to it even after an app update wipes the build defines
    _applyConnection(_connection.get('baseUrl') ?? _baseUrl, _connection.get('apiKey') ?? _apiKey);
  }

  void _applyConnection(String baseUrl, String apiKey) {
    _baseUrl = baseUrl.replaceFirst(RegExp(r'/+$'), '');
    _apiKey = apiKey;
    _dio.options.baseUrl = _baseUrl;
    if (_apiKey.isEmpty) {
      _dio.options.headers.remove('X-API-Key');
    } else {
      _dio.options.headers['X-API-Key'] = _apiKey;
    }
  }

  // Accepts "192.168.1.100", "icmis.local:8000" or a full URL and normalises it to .../api/v1
  static String normaliseBaseUrl(String input) {
    var url = input.trim();
    if (!url.startsWith(RegExp(r'https?://'))) url = 'http://$url';
    final uri = Uri.parse(url);
    final withPort = uri.hasPort ? uri : uri.replace(port: 8000);
    final path = withPort.path.replaceFirst(RegExp(r'/+$'), '');
    return withPort.replace(path: path.endsWith('/api/v1') ? path : '$path/api/v1').toString();
  }

  Future<void> updateConnection({required String baseUrl, required String apiKey}) async {
    _applyConnection(normaliseBaseUrl(baseUrl), apiKey.trim());
    await _connection.put('baseUrl', _baseUrl);
    await _connection.put('apiKey', _apiKey);
  }

  // Settings (config.yaml on the Pi; see api/routesSettings.py)
  Future<Map<String, dynamic>> getHealth() async => ((await _dio.get('/health')).data as Map).cast<String, dynamic>();

  Future<Map<String, dynamic>> getSettingsSchema() async => ((await _dio.get('/settings/schema')).data as Map).cast<String, dynamic>();

  Future<Map<String, dynamic>> getSettings() async => ((await _dio.get('/settings')).data as Map).cast<String, dynamic>();

  Future<Map<String, dynamic>> getSettingsStatus() async => ((await _dio.get('/settings/status')).data as Map).cast<String, dynamic>();

  Future<List<Map<String, dynamic>>> getSettingsBackups() async {
    final response = await _dio.get('/settings/backups');
    return ((response.data as Map)['backups'] as List? ?? []).whereType<Map>().map((item) => item.cast<String, dynamic>()).toList();
  }

  // The pre-flight check imports the affected programs on the Pi, which can take a while on a cold Pi
  Future<Map<String, dynamic>> saveSettings(Map<String, dynamic> values, String revision) async {
    final response = await _dio.patch('/settings',
        data: {'values': values, 'revision': revision}, options: Options(receiveTimeout: const Duration(minutes: 4)));
    return (response.data as Map).cast<String, dynamic>();
  }

  Future<Map<String, dynamic>> applySettings({List<String>? services}) async {
    final response = await _dio.post('/settings/apply', data: services == null ? <String, dynamic>{} : {'services': services});
    return (response.data as Map).cast<String, dynamic>();
  }

  Future<Map<String, dynamic>> rollbackSettings(String name) async {
    final response = await _dio.post('/settings/rollback', data: {'name': name}, options: Options(receiveTimeout: const Duration(minutes: 4)));
    return (response.data as Map).cast<String, dynamic>();
  }

  Future<Map<String, dynamic>> getSecrets() async {
    final response = await _dio.get('/settings/secrets');
    return (((response.data as Map)['secrets'] as Map?) ?? {}).cast<String, dynamic>();
  }

  Future<Map<String, dynamic>> saveSecret(String name, String value) async {
    final response = await _dio.put('/settings/secrets', data: {'name': name, 'value': value});
    return (response.data as Map).cast<String, dynamic>();
  }

  // Polls the public /health route until a restarted API answers again
  Future<bool> waitForApi({Duration timeout = const Duration(minutes: 3)}) async {
    final deadline = DateTime.now().add(timeout);
    await Future<void>.delayed(const Duration(seconds: 4));
    while (DateTime.now().isBefore(deadline)) {
      try {
        await getHealth();
        return true;
      } on DioException {
        await Future<void>.delayed(const Duration(seconds: 2));
      }
    }
    return false;
  }

  // Turns the API's {error: {message, detail}} envelope into per-field messages and a readable summary
  static ({String message, Map<String, String> fieldErrors}) describeError(Object error) {
    if (error is! DioException) return (message: '$error', fieldErrors: <String, String>{});
    final envelope = (error.response?.data is Map) ? (error.response!.data as Map)['error'] as Map? : null;
    final fieldErrors = <String, String>{};
    final detail = envelope?['detail'];
    if (detail is List) {
      for (final item in detail.whereType<Map>()) {
        fieldErrors['${item['key']}'] = '${item['message']}';
      }
    }
    if (envelope != null) {
      final message = fieldErrors.isNotEmpty ? fieldErrors.entries.map((entry) => entry.value).join(' ') : '${envelope['message']}';
      return (message: message, fieldErrors: fieldErrors);
    }
    final status = error.response?.statusCode;
    if (status == 401 || status == 403) return (message: 'The API key was rejected. Check it under Connection.', fieldErrors: fieldErrors);
    return (message: status == null ? 'The Pi is unreachable. Check the address under Connection and that the phone is on the Pi\'s network.' : 'The Pi returned HTTP $status.', fieldErrors: fieldErrors);
  }

  Future<List<RiskReport>> getReports({int limit = 20}) async {
    try {
      final response = await _dio.get('/reports', queryParameters: {'limit': limit});
      final items = ((response.data as Map)['items'] as List? ?? []).whereType<Map>().map((item) => RiskReport.fromJson(item.cast<String, dynamic>())).toList();
      await _cache.put('reports', jsonEncode(items.map((item) => {'id': item.id, 'species_id': item.speciesId, 'cri_score': item.criScore, 'scores': {'subscores': item.subscores}}).toList()));
      return items;
    } catch (_) {
      final cached = _cache.get('reports');
      if (cached == null) rethrow;
      return (jsonDecode(cached) as List).whereType<Map>().map((item) => RiskReport.fromJson(item.cast<String, dynamic>())).toList();
    }
  }

  Future<RiskReport> getReport(String reportId) async {
    final response = await _dio.get('/reports/$reportId', queryParameters: {'include_sources': true});
    return RiskReport.fromJson((response.data as Map).cast<String, dynamic>());
  }

  Future<List<TelemetryPoint>> getTelemetry({String readingType = 'gps', int limit = 250}) async {
    final response = await _dio.get('/telemetry/data', queryParameters: {'reading_type': readingType, 'limit': limit});
    return ((response.data as Map)['items'] as List? ?? []).whereType<Map>().map((item) => TelemetryPoint.fromJson(item.cast<String, dynamic>())).toList();
  }

  Future<Map<String, dynamic>> getSummary() async => (await _dio.get('/analytics/summary')).data as Map<String, dynamic>;

  Future<Map<String, dynamic>> getHistorical({String source = 'risk'}) async => (await _dio.get('/analytics/historical', queryParameters: {'source': source})).data as Map<String, dynamic>;

  Future<void> updateChecklist(String reportId, int index, bool completed) async {
    final operation = {'report_id': reportId, 'index': index, 'completed': completed};
    try {
      await _dio.patch('/reports/$reportId/checklist/$index', data: {'completed': completed});
    } on DioException {
      _pendingWrites.add(operation);
      await _queue.add(jsonEncode(operation));
    }
  }

  Future<void> flushPendingWrites() async {
    for (final operation in List<Map<String, dynamic>>.from(_pendingWrites)) {
      try {
        await _dio.patch('/reports/${operation['report_id']}/checklist/${operation['index']}', data: {'completed': operation['completed']});
        _pendingWrites.remove(operation);
      } on DioException {
        break;
      }
    }
    await _queue.clear();
    for (final operation in _pendingWrites) {
      await _queue.add(jsonEncode(operation));
    }
  }
}
