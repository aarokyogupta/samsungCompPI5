import 'dart:async';
import 'dart:convert';

import 'package:web_socket_channel/web_socket_channel.dart';

class AlertEvent {
  const AlertEvent(this.payload);
  final Map<String, dynamic> payload;
  String get priority => '${payload['priority'] ?? 'INFO'}';
  String get classification => '${payload['classification'] ?? payload['event_type'] ?? 'Alert'}';
  double get criScore => ((payload['metrics'] as Map?)?['cri_score'] as num?)?.toDouble() ?? 0;
}

class WebsocketService {
  WebsocketService({required this.baseUrl, required this.clientId, this.token});

  String baseUrl;
  final String clientId;
  String? token;
  final _events = StreamController<AlertEvent>.broadcast();
  WebSocketChannel? _channel;
  Timer? _reconnectTimer;
  int _attempt = 0;
  bool _closed = false;

  Stream<AlertEvent> get events => _events.stream;

  void connect() {
    _closed = false;
    _open();
  }

  // Called when the operator points the app at a different Pi or rotates the API key
  Future<void> reconfigure({required String baseUrl, String? token}) async {
    this.baseUrl = baseUrl;
    this.token = token;
    _closed = true;
    _reconnectTimer?.cancel();
    await _channel?.sink.close();
    _attempt = 0;
    connect();
  }

  void _open() {
    if (_closed) return;
    final uri = Uri.parse(baseUrl.replaceFirst(RegExp(r'^http'), 'ws')).replace(
      path: '${Uri.parse(baseUrl.replaceFirst(RegExp(r'^http'), 'ws')).path}/ws/alerts/$clientId',
      queryParameters: {if (token != null && token!.isNotEmpty) 'token': token!, 'client_type': 'android'},
    );
    try {
      _channel = WebSocketChannel.connect(uri);
      _channel!.stream.listen(_handleMessage, onDone: _scheduleReconnect, onError: (_) => _scheduleReconnect());
      _attempt = 0;
    } catch (_) {
      _scheduleReconnect();
    }
  }

  void _handleMessage(dynamic message) {
    final payload = jsonDecode('$message') as Map<String, dynamic>;
    if (payload['type'] == 'ping') {
      _channel?.sink.add(jsonEncode({'type': 'pong'}));
      return;
    }
    if (payload['event_id'] != null) _events.add(AlertEvent(payload));
  }

  void _scheduleReconnect() {
    if (_closed || _reconnectTimer?.isActive == true) return;
    final seconds = 1 << (_attempt++).clamp(0, 4).toInt();
    _reconnectTimer = Timer(Duration(seconds: seconds.clamp(1, 30)), _open);
  }

  Future<void> dispose() async {
    _closed = true;
    _reconnectTimer?.cancel();
    await _channel?.sink.close();
    await _events.close();
  }
}
