import 'package:flutter/material.dart';
import 'package:provider/provider.dart';
import 'services/apiService.dart';
import 'services/websocketService.dart';
import 'screens/analyticsScreen.dart';
import 'screens/homeScreen.dart';
import 'screens/mapScreen.dart';
import 'screens/settingsScreen.dart';

Future<void> main() async {
  WidgetsFlutterBinding.ensureInitialized();
  final api = ApiService();
  await api.initialise();
  // Uses the Pi address and key saved from the Settings tab, falling back to the build defines
  final websocket = WebsocketService(
    baseUrl: api.baseUrl,
    clientId: 'field-${DateTime.now().millisecondsSinceEpoch}',
    token: api.apiKey,
  )..connect();
  runApp(MultiProvider(providers: [
    Provider.value(value: api),
    Provider.value(value: websocket),
  ], child: const IcmisApp()));
}

class IcmisApp extends StatefulWidget {
  const IcmisApp({super.key});
  @override
  State<IcmisApp> createState() => _IcmisAppState();
}

class _IcmisAppState extends State<IcmisApp> {
  var index = 0;
  final screens = const [HomeScreen(), MapScreen(), AnalyticsScreen(), SettingsScreen()];
  @override
  Widget build(BuildContext context) => MaterialApp(
        title: 'ICMIS Field Operations',
        theme: ThemeData.dark(useMaterial3: true).copyWith(
          colorScheme: ColorScheme.fromSeed(seedColor: Colors.tealAccent, brightness: Brightness.dark),
          scaffoldBackgroundColor: const Color(0xFF08120F),
          cardTheme: const CardThemeData(color: Color(0xFF10251E)),
        ),
        home: Scaffold(body: IndexedStack(index: index, children: screens), bottomNavigationBar: NavigationBar(selectedIndex: index, onDestinationSelected: (value) => setState(() => index = value), destinations: const [
          NavigationDestination(icon: Icon(Icons.dashboard_outlined), label: 'Home'),
          NavigationDestination(icon: Icon(Icons.map_outlined), label: 'Map'),
          NavigationDestination(icon: Icon(Icons.analytics_outlined), label: 'Analytics'),
          NavigationDestination(icon: Icon(Icons.settings_outlined), label: 'Pi Settings'),
        ])),
      );
}
