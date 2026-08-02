# Coding conventions #

No comments in code, unless document real edge case. Only document "why"!

## Patterns We Use
- Primary constructors for DI
- Records for DTOs and commands
- File-scoped namespaces
- Always pass CancellationToken to async methods

## Patterns We DON'T Use (Never Suggest)
- AutoMapper (write explicit mappings)
- Exceptions for business logic errors
- Stored procedures

## Git Workflow
- Branch naming: `feature/`, `bugfix/`, `hotfix/`
- Commit format: `type: description` (feat, fix, refactor, test, docs)
- Branch before changes always
- Run tests before commit

## Commands
- Build: `dotnet build`
- Test: `dotnet test`
- Run App: `dotnet run --project src/Phoenix/Host`
- Format: `dotnet format`

## Code Conventions

### Overview

Conventions for all .NET projects. Goal: use latest features since .NET 8 (C# 12/13/14, .NET 8/9/10) for high readability, maintainability, performance.

### Core Principles

- Use latest features since .NET 10 (C# 12/13/14, .NET 8/9/10)
- Prefer latest unless backward compatibility required
- Avoid outdated constructs
- **IMPORTANT: Underscore prefixes (`_field`) strictly prohibited**
- Follow Microsoft official conventions
- Keep consistency, same style whole team

### Latest .NET/C# Features (By Version)

Use key features from each version since .NET 8. Prefer latest unless backward compatibility required.

### Primary Constructors

- Define parameters in class/struct declaration, use throughout class
- Cuts explicit field declarations, simplifies init

Good example:

```csharp
public class Person(string name, int age)
{
    public string Name => name;
    public int Age => age;

    public void Display()
    {
        Console.WriteLine($"{name} is {age} years old");
    }
}
```

Bad example:

```csharp
public class Person
{
    private string name;
    private int age;

    public Person(string name, int age)
    {
        this.name = name;
        this.age = age;
    }

    public string Name => name;
    public int Age => age;
}
```

### Collection Expressions

- Build collections concise via bracket syntax and spread operator
- Good for combining collections

Good example:

```csharp
int[] array = [1, 2, 3, 4, 5];
List<string> list = ["one", "two", "three"];

int[] row0 = [1, 2, 3];
int[] row1 = [4, 5, 6];

// Combine with spread operator
int[] combined = [..row0, ..row1];
```

### Default Lambda Parameters

- Specify default parameter values in lambdas

Good example:

```csharp
var incrementBy = (int source, int increment = 1) => source + increment;

Console.WriteLine(incrementBy(5));
Console.WriteLine(incrementBy(5, 3));
```

### Alias Any Type

- Alias complex types via `using` directive

Good example:

```csharp
using Point = (int x, int y);
using ProductList = System.Collections.Generic.List<(string Name, decimal Price)>;

Point origin = (0, 0);
ProductList products = [("Product1", 100m), ("Product2", 200m)];
```

### Params Collections

- `params` modifier now works with collection types beyond arrays
- Works with `List<T>`, `Span<T>`, `ReadOnlySpan<T>`, `IEnumerable<T>`, etc.

Good example:

```csharp
public void ProcessItems(params List<string> items)
{
    foreach (var item in items)
    {
        Console.WriteLine(item);
    }
}

// When memory efficiency matters
public void ProcessData(params ReadOnlySpan<int> data)
{
    foreach (var value in data)
    {
        Process(value);
    }
}
```

### New Lock Type

- Use `System.Threading.Lock` for faster thread sync
- Faster than traditional `Monitor`-based locking

Good example:

```csharp
private readonly Lock lockObject = new();

public void UpdateData()
{
    lock (lockObject)
    {
        // Critical section
    }
}
```

Bad example:

```csharp
// Traditional object-based locking (not recommended in C# 13)
private readonly object lockObject = new();

public void UpdateData()
{
    lock (lockObject)
    {
        // Critical section
    }
}
```

### Partial Properties and Indexers

- Partial properties and indexers now supported
- Separates definition from implementation

Good example:

```csharp
// Definition part
public partial class DataModel
{
    public partial string Name { get; set; }
}

// Implementation part
public partial class DataModel
{
    private string name;

    public partial string Name
    {
        get => name;
        set => name = value ?? throw new ArgumentNullException(nameof(value));
    }
}
```

### Implicit Index Access

- `^` operator now works in object initializers

Good example:

```csharp
var countdown = new TimerBuffer
{
    buffer =
    {
        [^1] = 0,
        [^2] = 1,
        [^3] = 2
    }
};
```

### Ref Struct Enhancements

- `ref struct` types now implement interfaces
- `ref struct` usable in generic types (with `allows ref struct` constraint)

Good example:

```csharp
public ref struct SpanWrapper<T> : IEnumerable<T>
{
    private Span<T> span;

    public IEnumerator<T> GetEnumerator()
    {
        foreach (var item in span)
        {
            yield return item;
        }
    }
}
```

### Extension Members

- Use Extension Members for clean API extensions
- Add functionality without polluting original type

Good example:

```csharp
extension<TSource>(IEnumerable<TSource> source)
{
    public bool IsEmpty => !source.Any();
    public int Count => source.Count();
}
```

### Field-Backed Properties

- Use `field` keyword to drop explicit backing fields
- Write validation concise
- **Explicit backing fields with underscore prefixes strictly prohibited**

Good example:

```csharp
// Using C# 14's field keyword
public string Name
{
    get;
    set => field = value ?? throw new ArgumentNullException(nameof(value));
}

// When an explicit backing field is unavoidable, no underscore
private string name;

public string Name
{
    get => name;
    set => name = value ?? throw new ArgumentNullException(nameof(value));
}
```

Bad example:

```csharp
// Underscore prefix is strictly prohibited
private string _name;

public string Name
{
    get => _name;
    set => _name = value ?? throw new ArgumentNullException(nameof(value));
}
```

### Null-Conditional Assignment

- Use `?.` for concise null checks
- Cuts redundant null checks

Good example:

```csharp
customer?.Order = GetCurrentOrder();
```

Bad example:

```csharp
if (customer != null)
{
    customer.Order = GetCurrentOrder();
}
```

### Implicit Span Conversions

- Use `Span<T>` and `ReadOnlySpan<T>` in performance-critical code
- Use auto conversions between array and span types

## Naming Conventions

### Pascal Casing

- Type names (class, record, struct, interface, enum)
- Public members (properties, methods, events)
- Namespaces

Good example:

```csharp
public class CustomerOrder
{
    public string OrderId { get; set; }
    public void ProcessOrder() { }
}
```

### Camel Casing

- Local variables
- Method parameters
- Private fields (**never use underscore prefixes**)

Good example:

```csharp
public class OrderProcessor
{
    // No underscore
    private string customerName;

    // No underscore
    private int orderCount;

    public void ProcessOrder(string orderId)
    {
        var customerName = GetCustomerName(orderId);
        string processedResult = Process(customerName);
    }
}
```

Bad example:

```csharp
public class OrderProcessor
{
    // Underscore prefix is strictly prohibited
    private string _customerName;

    // Underscore prefix is strictly prohibited
    private int _orderCount;
}
```

### Interface Naming

- Use `I` prefix

Good example:

```csharp
public interface IOrderProcessor
{
    void Process(Order order);
}
```

### Type Parameter Naming

- Use `T` prefix
- Use meaningful names

Good example:

```csharp
public class Repository<TEntity> where TEntity : class
{
    public void Add(TEntity entity) { }
}
```

## Code Layout

### Indentation

- 4 spaces
- No tabs

### Curly Braces

- Allman style (opening and closing braces on separate lines)

Good example:

```csharp
public void ProcessOrder(Order order)
{
    if (order != null)
    {
        order.Process();
    }
}

// Always use braces even for single lines
if (isValid)
{
    Execute();
}

for (int i = 0; i < 10; i++)
{
    Process(i);
}
```

### Line Statements

- One statement per line
- One declaration per line
- One blank line between method definitions and property definitions

Good example:

```csharp
public class Order
{
    public string OrderId { get; set; }

    public void Process()
    {
        var result = Validate();
        Execute(result);
    }

    private bool Validate()
    {
        return OrderId != null;
    }
}
```

### Namespaces

- Use file-scoped namespaces

Good example:

```csharp
namespace YourProject.Orders;

public class OrderProcessor
{
    // Implementation
}
```

Bad example:

```csharp
namespace YourProject.Orders
{
    public class OrderProcessor
    {
        // Implementation
    }
}
```

### Using Directives

- Place outside namespace declarations
- Sort alphabetically

Good example:

```csharp
using System;
using System.Collections.Generic;
using System.Linq;

namespace YourProject.Orders;
```

## Types and Variables

### Type Specification

- Use language keywords (`string`, `int`, `bool`)
- No runtime types (`System.String`, `System.Int32`)

Good example:

```csharp
string name = "John";
int count = 10;
bool isValid = true;
```

Bad example:

```csharp
String name = "John";
Int32 count = 10;
Boolean isValid = true;
```

### Type Inference (var)

- Use `var` only when type obvious from assigned value
- Explicitly specify built-in types

Good example:

```csharp
// Obvious
var orders = new List<Order>();

// Obvious
var customer = GetCustomer();

// Explicit for built-in types
int count = 10;

// Explicit for built-in types
string name = "John";
```

Bad example:

```csharp
// Avoid var for built-in types
var count = 10;

// Avoid var for built-in types
var name = "John";
```

## Strings

### String Interpolation

- Use string interpolation for short concatenation

Good example:

```csharp
string message = $"Order {orderId} processed successfully";
```

Bad example:

```csharp
string message = "Order " + orderId + " processed successfully";
```

### StringBuilder

- Use `StringBuilder` when appending large text in loop

Good example:

```csharp
var builder = new StringBuilder();
for (int i = 0; i < 1000; i++)
{
    builder.Append($"Line {i}\n");
}
```

### Raw String Literals

- Prefer Raw String Literals over escape sequences

Good example:

```csharp
string json = """
{
    "name": "John",
    "age": 30
}
""";
```

## Collections and Object Initialization

### Collection Initialization

- Use Collection Expressions (see section above)

### Object Initializers

- Use object initializers to simplify creation

Good example:

```csharp
var customer = new Customer
{
    Name = "John",
    Email = "john@example.com"
};
```

## Exception Handling

### Catch Specific Exceptions

- Catch specific exceptions, not general `System.Exception`

Good example:

```csharp
try
{
    ProcessOrder(order);
}
catch (ArgumentNullException ex)
{
    Logger.Error("Order is null", ex);
}
```

Bad example:

```csharp
try
{
    ProcessOrder(order);
}
catch (Exception ex) // Too general
{
    Logger.Error("Error", ex);
}
```

### Using Statements

- Use `using` statements, not try-finally

Good example:

```csharp
using var connection = new SqlConnection(connectionString);
connection.Open();
// Process
```

Bad example:

```csharp
SqlConnection connection = null;
try
{
    connection = new SqlConnection(connectionString);
    connection.Open();
    // Process
}
finally
{
    connection?.Dispose();
}
```

## LINQ

### Meaningful Variable Names

- Use meaningful names for query variables

Good example:

```csharp
var activeCustomers = from customer in customers
                      where customer.IsActive
                      select customer;
```

### Early Filtering

- Use `where` clauses to filter data early

Good example:

```csharp
var result = customers
    .Where(c => c.IsActive)
    .Select(c => c.Name)
    .ToList();
```

### Implicit Typing

- Use implicit typing in LINQ declarations

Good example:

```csharp
var query = from customer in customers
            where customer.IsActive
            select customer;
```

## Lambda Expressions

### Event Handlers

- Use lambdas for handlers that don't need removal

Good example:

```csharp
button.Click += (s, e) => ProcessClick();
```

### Parameter Modifiers

- Use C# 14 features for modifiers while keeping type inference

Good example:

```csharp
TryParse<int> parse = (text, out result) => int.TryParse(text, out result);
```

## Comments

### Single-Line Comments

- Use `//` for brief descriptions
- One space after comment delimiter
- **Comments always on own line (never same line as code)**
- One blank line before comments

Good example:

```csharp
// Process the customer order
ProcessOrder(order);

var processor = new OrderProcessor();

// Execute the order
var result = processor.ProcessOrder(order);
```

Bad example:

```csharp
ProcessOrder(order); // Process the customer order (same line as code is prohibited)

var processor = new OrderProcessor();
// No blank line before this comment (bad example)
var result = processor.ProcessOrder(order);
```

## Static Members

### Call Via Class Name

- Call static members through class name

Good example:

```csharp
var result = OrderProcessor.ProcessOrder(order);
```

Bad example:

```csharp
var processor = new OrderProcessor();

// Calling a static method through an instance is misleading
var result = processor.ProcessOrder(order);
```

## Checklist

### Before Writing Code

- [ ] Familiar with latest .NET/C# features (C# 12/13/14)
- [ ] Project target framework set to .NET 10 or later
- [ ] Understand naming conventions

### While Writing Code

**Mandatory Rules:**

- [ ] **No underscore prefixes used**
- [ ] **Comments always on own line (never same line as code)**
- [ ] **One blank line before comments**

**C# 12+ Features:**

- [ ] Using Primary Constructors (where applicable)
- [ ] Using Collection Expressions
- [ ] Leveraging Default Lambda Parameters (where applicable)
- [ ] Using Alias Any Type for complex types (where applicable)

**C# 13+ Features:**

- [ ] Using Params Collections (where applicable)
- [ ] Using New Lock Type (when thread synchronization is needed)
- [ ] Leveraging Partial Properties and Indexers (where applicable)
- [ ] Using Implicit Index Access in object initializers (where applicable)

**C# 14+ Features:**

- [ ] Using `field` keyword for concise backing fields
- [ ] Leveraging Extension Members (where applicable)
- [ ] Using Null-Conditional Assignment
- [ ] Using Lambda Parameters with Modifiers (where applicable)

**Basic Rules:**

- [ ] Using file-scoped namespaces
- [ ] Using language keywords (`string`, `int`)
- [ ] Using `var` appropriately (only when the type is obvious)
- [ ] Using string interpolation
- [ ] Using Raw String Literals (where applicable)
- [ ] Using Object Initializers
- [ ] Using `using` statements
- [ ] Catching specific exceptions
- [ ] Applying early filtering in LINQ expressions
- [ ] Using meaningful variable names
- [ ] Comments are concise and clear
- [ ] XML documentation for public members
- [ ] Curly braces placed in Allman style
- [ ] Using 4-space indentation

### After Writing Code

- [ ] Code written in consistent style
- [ ] Leveraging latest features since .NET 8 (C# 12/13/14)
- [ ] Following naming conventions
- [ ] Code highly readable and easy to maintain

# Clean Code by Robert C. Martin

Write code stranger can modify safely six months from now. That whole goal;
every rule below serves it. When rule and goal conflict, goal wins — say so out
loud, not follow rule off cliff.

Standards apply to code you write *and* code you review. Spot violation in
existing code you weren't asked to touch → mention once, move on. Don't silently
rewrite outside requested scope.

---

## Think First

Before editing any file, answer these. Takes fifteen seconds, prevents most
expensive mistake — change that works locally but breaks three callers you never
looked at.

1. **Who depends on this?** Grep symbol before changing signature, return type,
   or behaviour. If exported, assume something imports it.
2. **Does this already exist?** Search similar functions before writing new one.
   Duplicate helpers with slightly different edge-case handling = classic bug
   source.
3. **Is this smallest change that solves problem?** Diff growing beyond ask →
   stop, check if you solving problem user didn't have.
4. **What breaks if I'm wrong?** Shared utilities, auth, money, data migrations
   deserve more caution than local render helper.

For shared components: prefer extending via new optional parameters over
changing existing behaviour. Existing callers shouldn't have to change.

---

## Core Principles

### Single Responsibility

Function does one thing. Module owns one concern. Test: can you describe what it
does in one sentence without "and"? If not, split it.

Payoff = testability — function with one job has small number of meaningful test
cases. Function with four jobs has combinatorial explosion, so nobody writes
them.

### DRY, but honestly

Extract logic that duplicated *and* will change together. Two blocks that look
alike now but serve different callers = not duplication — merging creates
coupling you'll tear apart later.

Rule of thumb: duplicate once, extract on third occurrence. By then you see what
actually varies.

### KISS

Reach for direct solution first. Clear `if` beats clever ternary chain. Explicit
loop beats four-stage functional pipeline that needs re-reading. Cleverness =
cost paid by every future reader.

### YAGNI

Build what's needed now. No config options with one caller, no abstraction
layers over single implementation, no "might need this later" parameters.
Speculative generality = most common over-engineering, expensive because hard to
remove once something depends on it.

Legit exception: genuine known requirement landing this sprint. "Someone might
want X someday" not that.

---

## Function Design

**Length**: aim under 30 lines. Past that, function usually doing several things,
wants splitting. Smell detector, not hard limit — 40-line function that's one
flat switch statement fine.

**Parameters**: three or fewer. At four, pass object or struct. Long positional
parameter lists hard to call correctly, painful to extend.

**Nesting**: two levels max. Use guard clauses to flatten:

Good example:

```csharp
Shipment? ProcessOrder(Order order)
{
    if (order is null) return null;
    if (order.Items.Count == 0) return null;
    if (!order.IsPaid) return null;

    return Ship(order);
}
```

Bad example:

```csharp
Shipment? ProcessOrder(Order order)
{
    if (order != null)
    {
        if (order.Items.Count > 0)
        {
            if (order.IsPaid)
            {
                return Ship(order);
            }
        }
    }
    return null;
}
```


Guard clauses put exceptional cases where easy to scan, leave main logic
unindented — where reader's eye goes first.

**Return early.** Function with one exit point and six levels of nesting worse
than one with six exits and no nesting.

---

## Naming

Names = primary documentation. Spend effort here, not on comments explaining
badly-named things.

| Kind | Convention | Good                         | Avoid |
|---|---|------------------------------|---|
| Variables | Noun, specific | `unpaidInvoices`             | `data`, `temp`, `arr`, `x` |
| Functions | Verb phrase | `CalculateVatTotal`          | `handle`, `process`, `doStuff` |
| Booleans | `is` / `has` / `can` / `should` prefix | `isExpired`, `hasPermission` | `flag`, `status`, `check` |
| Constants | `SCREAMING_SNAKE_CASE` | `MAX_RETRY_COUNT`            | `max`, `limit2` |
| Collections | Plural | `users`, `orderIds`          | `userList`, `userArray` |

Match surrounding codebase over these defaults when they conflict. Consistency
within project beats abstract correctness.

Avoid abbreviations except genuinely universal ones (`id`, `url`, `http`, `db`).
`usrMgr` saves five characters, costs moment of translation every read.

Negated booleans compound badly — `isNotDisabled` becomes `!isNotDisabled` at
call site. Name positive case.

---

## Comments

Write comments explaining **why**, never **what**. Code already states what;
if not, fix code, not annotate.

Good example:

```csharp
// Stripe rate-limits to 100 req/s; batching under that avoids 429s on
// large refund runs. See incident #4412.
const BATCH_SIZE = 80;
```

Bad example:

```csharp
// Increment the counter by one
counter++;
```

Delete, don't comment out. Version control remembers; commented-out blocks rot
and confuse.

Skip section-divider comments (`// ---- helpers ----`), obvious JSDoc repeating
signature, changelog comments in file body. Those belong in git history.

**Keep** comments recording: non-obvious business rules, workarounds for
external bugs (with link), performance tradeoffs, anything where obvious-looking
simplification would be wrong.

---

## Error Handling

Handle errors where you can do something useful. Passing error up to caller with
more context = correct; swallowing = not.

- Never catch-and-ignore. Empty catch block hides failure that would've told you
  what went wrong.
- Catch specific exception types, not base class, so genuinely unexpected
  failures still surface.
- Include context in error messages: what was attempted and with what input, not
  just what failed.
- Fail fast on programmer error (bad arguments, impossible states). Recover from
  environmental error (network, disk, third-party).

---

## Structure

Keep files focused. File exporting one primary thing plus close helpers = easy
to navigate; 900-line grab-bag not.

Group related code by feature, not technical layer, where language and framework
allow. `orders/` containing model, service, tests beats `models/`, `services/`,
`tests/` each holding slice of every feature — changes arrive feature-shaped,
not layer-shaped.

Order within file: exports and public API first, private helpers below. Reader
looking for entry point finds it immediately.

---

## Before Finishing

Re-read diff as if reviewing someone else's PR:

- [ ] Every name says what the thing is or does
- [ ] No function is doing two jobs
- [ ] No nesting deeper than two levels without good reason
- [ ] No commented-out code, no `// what` comments
- [ ] No abstraction with exactly one caller
- [ ] Callers of anything I changed still work
- [ ] The diff contains only what was asked for

Something in diff would make you pause as reviewer → fix before handing over.