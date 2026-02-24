using System;
using System.Reflection;
using System.Linq;

var asm = System.Reflection.Assembly.LoadFrom(@"D:\X\NewLife.NovaDb\Bin\UnitTest\net8.0\NewLife.Core.dll");
Type[] types;
try { types = asm.GetTypes(); } catch (ReflectionTypeLoadException ex) { types = ex.Types.Where(t => t != null).ToArray(); }

foreach (var t in types.Where(t => t.Name == "SpanWriter" || t.Name == "SpanReader" || t.Name == "OwnerPacket" || t.Name == "IOwnerPacket"))
{
    Console.WriteLine($"\n=== {t.FullName} (Struct={t.IsValueType}) ===");
    foreach (var c in t.GetConstructors()) Console.WriteLine($"  ctor({string.Join(", ", c.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"))})");
    foreach (var p in t.GetProperties()) Console.WriteLine($"  prop {p.PropertyType.Name} {p.Name}");
    foreach (var m in t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly))
        if (!m.IsSpecialName) Console.WriteLine($"  {m.ReturnType.Name} {m.Name}({string.Join(", ", m.GetParameters().Select(p => $"{p.ParameterType.Name} {p.Name}"))})");
}
