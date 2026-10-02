import 'dart:async';
import 'dart:convert';

import 'package:dio/dio.dart';
import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import '../services/apiService.dart';
import '../services/websocketService.dart';

// Mirrors webDashboard/src/pages/Settings.jsx so both clients edit config.yaml on the Pi the same way
class SettingsScreen extends StatefulWidget {
  const SettingsScreen({super.key});
  @override
  State<SettingsScreen> createState() => _SettingsScreenState();
}

class _SettingsScreenState extends State<SettingsScreen> {
  // Schema and values from GET /settings/schema and GET /settings
  List<Map<String, dynamic>> groups = [];
  List<Map<String, dynamic>> fields = [];
  Map<String, dynamic> values = {};
  String revision = '';
  List<String> pending = [];
  Map<String, dynamic>? status;
  Map<String, dynamic> secrets = {};
  List<Map<String, dynamic>> backups = [];

  // Form state: drafts keep raw text so half-typed numbers survive a rebuild
  final Map<String, dynamic> drafts = {};
  Map<String, String> fieldErrors = {};
  String? message;
  bool busy = false;
  bool loading = true;
  Timer? statusTimer;

  late final TextEditingController urlController;
  late final TextEditingController keyController;
  final Map<String, TextEditingController> secretControllers = {};

  @override
  void initState() {
    super.initState();
    final api = context.read<ApiService>();
    urlController = TextEditingController(text: api.baseUrl);
    keyController = TextEditingController(text: api.apiKey);
    _loadEverything();
    statusTimer = Timer.periodic(const Duration(seconds: 10), (_) => _refreshStatus());
  }

  @override
  void dispose() {
    statusTimer?.cancel();
    urlController.dispose();
    keyController.dispose();
    for (final controller in secretControllers.values) {
      controller.dispose();
    }
    super.dispose();
  }

  // Conversions (same rules as toDraft/fromDraft in the dashboard)
  dynamic _toDraft(Map<String, dynamic> field, dynamic value) {
    final valueType = field['valueType'];
    if (value == null) return valueType == 'boolean' ? false : '';
    if (valueType == 'stringList') return (value as List).join('\n');
    if (valueType == 'json') return const JsonEncoder.withIndent('  ').convert(value);
    if (valueType == 'boolean') return value == true;
    return '$value';
  }

  dynamic _fromDraft(Map<String, dynamic> field, dynamic draft) {
    final valueType = field['valueType'];
    final text = '$draft';
    if (valueType == 'boolean') return draft == true;
    if (valueType == 'stringList') return text.split(RegExp(r'[\n,]')).map((item) => item.trim()).where((item) => item.isNotEmpty).toList();
    // A FormatException is caught by the caller and shown next to the field
    if (valueType == 'json') return jsonDecode(text.trim().isEmpty ? 'null' : text);
    if (valueType == 'integer' || valueType == 'number') {
      if (text.trim().isEmpty) return null;
      return num.tryParse(text.trim()) ?? text;
    }
    if (field['nullable'] == true && text.trim().isEmpty) return null;
    return text;
  }

  bool _sameValue(dynamic left, dynamic right) => jsonEncode(left) == jsonEncode(right);

  Map<String, dynamic> _changedValues() {
    final changed = <String, dynamic>{};
    final errors = <String, String>{};
    for (final field in fields) {
      final key = field['key'] as String;
      if (!drafts.containsKey(key)) continue;
      try {
        final value = _fromDraft(field, drafts[key]);
        if (!_sameValue(value, values[key])) changed[key] = value;
      } on FormatException catch (error) {
        errors[key] = 'Invalid JSON: ${error.message}';
      }
    }
    if (errors.isNotEmpty) throw _DraftErrors(errors);
    return changed;
  }

  bool _isDirty(Map<String, dynamic> field) {
    final key = field['key'] as String;
    try {
      return !_sameValue(_fromDraft(field, drafts[key]), values[key]);
    } on FormatException {
      return true;
    }
  }

  // Network actions
  Future<void> _loadEverything() async {
    final api = context.read<ApiService>();
    setState(() {
      loading = true;
      message = null;
    });
    try {
      final schema = await api.getSettingsSchema();
      final current = await api.getSettings();
      groups = ((schema['groups'] as List?) ?? []).whereType<Map>().map((item) => item.cast<String, dynamic>()).toList();
      fields = ((schema['fields'] as List?) ?? []).whereType<Map>().map((item) => item.cast<String, dynamic>()).toList();
      secrets = ((schema['secrets'] as Map?) ?? {}).cast<String, dynamic>();
      _acceptValues(current);
      backups = await api.getSettingsBackups();
      await _refreshStatus();
    } catch (error) {
      message = ApiService.describeError(error).message;
    }
    if (mounted) setState(() => loading = false);
  }

  void _acceptValues(Map<String, dynamic> current) {
    values = ((current['values'] as Map?) ?? {}).cast<String, dynamic>();
    revision = '${current['revision'] ?? ''}';
    pending = ((current['pending'] as List?) ?? []).map((item) => '$item').toList();
    drafts.clear();
    for (final field in fields) {
      drafts[field['key'] as String] = _toDraft(field, values[field['key']]);
    }
    fieldErrors = {};
  }

  Future<void> _refreshStatus() async {
    try {
      final latest = await context.read<ApiService>().getSettingsStatus();
      if (mounted) setState(() => status = latest);
    } catch (_) {
      // The status card simply keeps its last snapshot while the Pi is restarting
    }
  }

  Future<void> _run(Future<void> Function() action) async {
    setState(() {
      busy = true;
      message = null;
    });
    try {
      await action();
    } on _DraftErrors catch (error) {
      fieldErrors = error.errors;
      message = 'Fix the highlighted fields first.';
    } catch (error) {
      final described = ApiService.describeError(error);
      fieldErrors = described.fieldErrors;
      message = described.message;
      // 409 means someone else saved from another device; reload so the operator sees their values
      if (error is DioException && error.response?.statusCode == 409) await _loadEverything();
    }
    if (mounted) setState(() => busy = false);
  }

  Future<void> _saveConnection() => _run(() async {
        final api = context.read<ApiService>();
        final websocket = context.read<WebsocketService>();
        await api.updateConnection(baseUrl: urlController.text, apiKey: keyController.text);
        urlController.text = api.baseUrl;
        await websocket.reconfigure(baseUrl: api.baseUrl, token: api.apiKey);
        await api.getHealth();
        await _loadEverything();
        message ??= 'Connected to ${api.baseUrl}.';
      });

  Future<void> _save() => _run(() async {
        final changed = _changedValues();
        if (changed.isEmpty) {
          message = 'Nothing to save.';
          return;
        }
        final api = context.read<ApiService>();
        final result = await api.saveSettings(changed, revision);
        _acceptValues(result);
        backups = await api.getSettingsBackups();
        message = 'Saved ${changed.length} setting(s). Press "Apply now" to restart the affected programs.';
      });

  Future<void> _apply({List<String>? services}) => _run(() async {
        final api = context.read<ApiService>();
        final result = await api.applySettings(services: services);
        if (result['apiRestarting'] == true) {
          if (mounted) setState(() => message = 'The API is restarting; reconnecting…');
          final back = await api.waitForApi();
          if (!back) throw Exception('The API did not come back within 3 minutes. Check "journalctl -u icmis-api" on the Pi.');
        } else {
          await Future<void>.delayed(const Duration(seconds: 3));
        }
        _acceptValues(await api.getSettings());
        await _refreshStatus();
        message = 'Changes applied on the Pi.';
      });

  Future<void> _rollback(String name) => _run(() async {
        final api = context.read<ApiService>();
        final result = await api.rollbackSettings(name);
        _acceptValues(result);
        backups = await api.getSettingsBackups();
        message = 'Restored $name. Press "Apply now" to use it.';
      });

  Future<void> _saveSecret(String name) => _run(() async {
        final api = context.read<ApiService>();
        final websocket = context.read<WebsocketService>();
        final controller = secretControllers[name]!;
        final value = controller.text.trim();
        await api.saveSecret(name, value);
        secrets = await api.getSecrets();
        controller.clear();
        if (name == 'ICMIS_API_KEY') {
          // The old key stays valid until the API restarts, so restart first and switch this phone over after
          await api.applySettings(services: ['api']);
          await api.waitForApi();
          await api.updateConnection(baseUrl: api.baseUrl, apiKey: value);
          keyController.text = value;
          await websocket.reconfigure(baseUrl: api.baseUrl, token: value);
        }
        message = 'Saved $name.';
      });

  // UI
  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text('Pi Settings'), actions: [
        IconButton(onPressed: busy ? null : _loadEverything, icon: const Icon(Icons.refresh), tooltip: 'Reload'),
      ]),
      floatingActionButton: fields.isEmpty
          ? null
          : FloatingActionButton.extended(onPressed: busy ? null : _save, icon: const Icon(Icons.save_outlined), label: const Text('Save')),
      body: RefreshIndicator(
        onRefresh: _loadEverything,
        child: ListView(padding: const EdgeInsets.fromLTRB(16, 16, 16, 96), children: [
          if (busy) const LinearProgressIndicator(),
          if (message != null) Card(child: ListTile(leading: const Icon(Icons.info_outline), title: Text(message!))),
          _connectionCard(),
          if (pending.isNotEmpty) _pendingCard(),
          if (loading && fields.isEmpty) const Padding(padding: EdgeInsets.all(24), child: Center(child: CircularProgressIndicator())),
          for (final group in groups) _groupTile(group),
          if (status != null) _statusCard(),
          if (secrets.isNotEmpty) _secretsCard(),
          if (backups.isNotEmpty) _backupsCard(),
        ]),
      ),
    );
  }

  Widget _connectionCard() => Card(
        child: Padding(
          padding: const EdgeInsets.all(12),
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            const Text('Connection', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
            const Text('Address of the Pi (e.g. icmis.local or 192.168.1.100) and the API key printed by installServices.sh.'),
            TextField(controller: urlController, decoration: const InputDecoration(labelText: 'Pi address'), keyboardType: TextInputType.url),
            TextField(controller: keyController, decoration: const InputDecoration(labelText: 'API key'), obscureText: true),
            const SizedBox(height: 8),
            FilledButton.icon(onPressed: busy ? null : _saveConnection, icon: const Icon(Icons.link), label: const Text('Save & test connection')),
          ]),
        ),
      );

  Widget _pendingCard() {
    return Card(
      color: Colors.amber.withValues(alpha: 0.15),
      child: ListTile(
        leading: const Icon(Icons.pending_actions, color: Colors.amber),
        title: const Text('Saved changes are waiting to be applied'),
        subtitle: Text('Programs to restart: ${pending.join(', ')}'),
        trailing: FilledButton(onPressed: busy || status?['applyAvailable'] == false ? null : () => _apply(), child: const Text('Apply now')),
      ),
    );
  }

  Widget _groupTile(Map<String, dynamic> group) {
    final groupFields = fields.where((field) => field['group'] == group['id']).toList();
    if (groupFields.isEmpty) return const SizedBox.shrink();
    final dirty = groupFields.where(_isDirty).length;
    final errors = groupFields.where((field) => fieldErrors.containsKey(field['key'])).length;
    return Card(
      child: ExpansionTile(
        title: Text('${group['label'] ?? group['id']}'),
        subtitle: Text([
          if (group['description'] != null) '${group['description']}',
          if (dirty > 0) '$dirty unsaved',
          if (errors > 0) '$errors error(s)',
        ].join(' · ')),
        childrenPadding: const EdgeInsets.fromLTRB(12, 0, 12, 12),
        children: [for (final field in groupFields) _fieldInput(field)],
      ),
    );
  }

  Widget _fieldInput(Map<String, dynamic> field) {
    final key = field['key'] as String;
    final valueType = field['valueType'];
    final error = fieldErrors[key];
    final dirty = _isDirty(field);
    final label = '${field['label'] ?? key}${dirty ? ' •' : ''}';
    final range = [
      if (field['minimum'] != null) 'min ${field['minimum']}',
      if (field['maximum'] != null) 'max ${field['maximum']}',
      if (field['services'] is List && (field['services'] as List).isNotEmpty) 'restarts ${(field['services'] as List).join(', ')}',
    ].join(' · ');
    final helper = [if (field['description'] != null) '${field['description']}', if (range.isNotEmpty) range].join('\n');
    void update(dynamic value) => setState(() {
          drafts[key] = value;
          fieldErrors.remove(key);
        });

    if (valueType == 'boolean') {
      return SwitchListTile(
        contentPadding: EdgeInsets.zero,
        title: Text(label),
        subtitle: Text(error ?? helper, style: TextStyle(color: error != null ? Colors.redAccent : null)),
        value: drafts[key] == true,
        onChanged: (value) => update(value),
      );
    }
    if (valueType == 'enum') {
      final options = ((field['options'] as List?) ?? []).map((item) => '$item').toList();
      final current = '${drafts[key] ?? ''}';
      return Padding(
        padding: const EdgeInsets.symmetric(vertical: 6),
        child: DropdownButtonFormField<String>(
          value: options.contains(current) || current.isEmpty ? current : null,
          isExpanded: true,
          decoration: InputDecoration(labelText: label, helperText: helper, helperMaxLines: 4, errorText: error),
          items: [
            if (field['nullable'] == true) const DropdownMenuItem(value: '', child: Text('(not set)')),
            for (final option in options) DropdownMenuItem(value: option, child: Text(option)),
          ],
          onChanged: (value) => update(value ?? ''),
        ),
      );
    }
    final multiline = valueType == 'json' || valueType == 'stringList';
    final numeric = valueType == 'integer' || valueType == 'number';
    // Key keeps the field's cursor when setState rebuilds; the draft text is the source of truth
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 6),
      child: TextFormField(
        key: ValueKey('$key@$revision'),
        initialValue: '${drafts[key] ?? ''}',
        minLines: multiline ? 2 : 1,
        maxLines: multiline ? 8 : 1,
        keyboardType: numeric ? const TextInputType.numberWithOptions(decimal: true, signed: true) : TextInputType.text,
        style: multiline ? const TextStyle(fontFamily: 'monospace', fontSize: 13) : null,
        decoration: InputDecoration(labelText: label, helperText: helper.isEmpty ? null : helper, helperMaxLines: 4, errorText: error, errorMaxLines: 4),
        onChanged: update,
      ),
    );
  }

  Widget _statusCard() {
    final workers = ((status!['workers'] as List?) ?? []).whereType<Map>().toList();
    final schedules = ((status!['schedules'] as List?) ?? []).whereType<Map>().toList();
    final lastApply = status!['lastApply'] as Map?;
    Color stateColour(String? state) => switch (state) {
          'active' => Colors.greenAccent,
          'activating' || 'reloading' => Colors.amber,
          'failed' => Colors.redAccent,
          _ => Colors.grey,
        };
    return Card(
      child: Padding(
        padding: const EdgeInsets.all(12),
        child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
          const Text('Programs on the Pi', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
          if (status!['systemdAvailable'] == false) const Text('systemd is not available here, so live service states cannot be shown.'),
          if (lastApply != null)
            Text('Last apply: ${lastApply['state']}${lastApply['error'] != null ? ' – ${lastApply['error']}' : ''}',
                style: TextStyle(color: lastApply['state'] == 'failed' ? Colors.redAccent : null)),
          for (final worker in workers)
            ListTile(
              contentPadding: EdgeInsets.zero,
              leading: Icon(Icons.circle, size: 12, color: stateColour(worker['activeState'] as String?)),
              title: Text('${worker['label'] ?? worker['service']}'),
              subtitle: Text([
                '${worker['activeState'] ?? 'unknown'}${worker['subState'] != null ? ' (${worker['subState']})' : ''}',
                if (worker['ready'] == false && worker['reason'] != null) 'Skipped: ${worker['reason']}',
              ].join('\n')),
              trailing: IconButton(
                icon: const Icon(Icons.restart_alt),
                tooltip: 'Restart',
                onPressed: busy || status!['applyAvailable'] == false ? null : () => _apply(services: ['${worker['service']}']),
              ),
            ),
          const Divider(),
          const Text('Schedules', style: TextStyle(fontWeight: FontWeight.bold)),
          for (final schedule in schedules)
            ListTile(
              contentPadding: EdgeInsets.zero,
              leading: Icon(schedule['configEnabled'] == false ? Icons.timer_off_outlined : Icons.timer_outlined),
              title: Text('${schedule['label'] ?? schedule['schedule']}'),
              subtitle: Text('${schedule['configEnabled'] == false ? 'Disabled' : 'Runs ${schedule['onCalendar'] ?? '?'}'}'
                  '${schedule['activeState'] != null ? ' · timer ${schedule['activeState']}' : ''}'),
            ),
        ]),
      ),
    );
  }

  Widget _secretsCard() => Card(
        child: Padding(
          padding: const EdgeInsets.all(12),
          child: Column(crossAxisAlignment: CrossAxisAlignment.start, children: [
            const Text('Secrets (.env on the Pi)', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
            const Text('Write-only: the Pi never sends these values back.'),
            for (final entry in secrets.entries) _secretRow(entry.key, (entry.value as Map).cast<String, dynamic>()),
          ]),
        ),
      );

  Widget _secretRow(String name, Map<String, dynamic> secret) {
    final controller = secretControllers.putIfAbsent(name, TextEditingController.new);
    return Row(children: [
      Expanded(
        child: TextField(
          controller: controller,
          obscureText: true,
          decoration: InputDecoration(
            labelText: '${secret['label'] ?? name}',
            helperText: '${secret['isSet'] == true ? 'Set' : 'Not set'}${secret['minLength'] != null ? ' · at least ${secret['minLength']} characters' : ''}',
          ),
        ),
      ),
      IconButton(icon: const Icon(Icons.save_outlined), onPressed: busy ? null : () => _saveSecret(name)),
    ]);
  }

  Widget _backupsCard() => Card(
        child: ExpansionTile(
          title: const Text('Config backups'),
          subtitle: Text('${backups.length} saved on the Pi'),
          children: [
            for (final backup in backups)
              ListTile(
                title: Text('${backup['name']}'),
                subtitle: Text('${backup['createdAt'] ?? ''}'),
                trailing: TextButton(onPressed: busy ? null : () => _confirmRollback('${backup['name']}'), child: const Text('Restore')),
              ),
          ],
        ),
      );

  Future<void> _confirmRollback(String name) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (dialogContext) => AlertDialog(
        title: const Text('Restore backup?'),
        content: Text('config.yaml on the Pi will be replaced with $name. The current file is backed up first.'),
        actions: [
          TextButton(onPressed: () => Navigator.pop(dialogContext, false), child: const Text('Cancel')),
          FilledButton(onPressed: () => Navigator.pop(dialogContext, true), child: const Text('Restore')),
        ],
      ),
    );
    if (confirmed == true) await _rollback(name);
  }
}

class _DraftErrors implements Exception {
  const _DraftErrors(this.errors);
  final Map<String, String> errors;
}
