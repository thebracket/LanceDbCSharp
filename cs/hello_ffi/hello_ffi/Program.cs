// Minimal FFI example in C#

using System.Runtime.InteropServices;

// We're assuming a build path of "/home/herbert/Rust/lancedb_csharp/cs/hello_ffi/hello_ffi/bin/Debug/net8.0/"
// Rust will put the shared library in lancedb_csharp/rust/target/debug/
// The shared library is named "libtest_library.so"
// So apologies for the hideous relative path, I wanted a simple example that would
// work without any additional setup.

// Path to the shared library
const string dllName = "../../../../../../rust/target/debug/libtest_library.so";

// Import the add function from the shared library
[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern int add(int a, int b);

[DllImport(dllName, CallingConvention = CallingConvention.Cdecl)]
static extern void print(string text);

// Call the add function
int result = add(1, 2);
System.Console.WriteLine(result);

// Call the print function
print("Hello from C#!");
