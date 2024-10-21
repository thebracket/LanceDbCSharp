// Note that this is a temporary console app - it'll become a proper
// library in the future.

using System.Runtime.InteropServices;

// Path to the shared library
const string dllName = "../../../../../../rust/target/debug/liblance_sync_client.so";

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern int setup();

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern int shutdown();

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern long connect(string uri);

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern long disconnect(long handle);

var r = setup();
System.Console.WriteLine("Setup: " + r);
Int64 conn = connect("data/sample_db");
System.Console.WriteLine("Connection: " + conn);
var d = disconnect(conn);
System.Console.WriteLine("Disconnect: " + d);
r = shutdown();
System.Console.WriteLine("Shutdown: " + r);
