[![](https://img.shields.io/nuget/v/soenneker.extensions.iqueryables.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.extensions.iqueryables/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.extensions.iqueryables/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.extensions.iqueryables/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.extensions.iqueryables.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.extensions.iqueryables/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.extensions.iqueryables/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.extensions.iqueryables/actions/workflows/codeql.yml)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.Extensions.IQueryables
Builds deferred equality, range, text-search, and ordering expressions from runtime property names.

## Installation

```bash
dotnet add package Soenneker.Extensions.IQueryables
```

## Filter by a runtime field

```csharp
using Soenneker.Extensions.IQueryables;

IQueryable<Order> query = dbContext.Orders;

query = query.WhereDynamicEquals("Status", "Open");
query = query.WhereDynamicRange(new RangeFilter
{
    Field = "Total",
    GreaterThanOrEqual = 100m,
    LessThan = 500m
});
```

Field names are case-insensitive and can use either CLR property names or `[JsonPropertyName]` aliases. Dotted paths such as `"Customer.Name"` traverse public instance properties. Values are converted to the selected property type using invariant culture, including enums, GUIDs, dates, and nullable value types.

An equality comparison against `null` matches nullable/reference properties that are null. It never matches a non-nullable value property. Range bounds are combined with `AND`; a range with no bounds leaves the query unchanged.

Unknown fields, malformed paths, and values that cannot be converted fail while composing the query. Treat field names exposed through an API as an allowlisted contract rather than passing arbitrary client strings unchecked.

## Search string properties

```csharp
query = query.WhereDynamicSearch(
    search: "acme",
    fields: ["Customer.Name", "Reference"]);
```

Search creates an `OR` of `string.Contains(search)` across valid string fields. Blank field names and non-string properties are skipped; null string values do not match. Matching semantics, collation, and translation depend on the `IQueryable` provider. A blank search or empty field list leaves the query unchanged.

For LINQ-to-Objects, every intermediate object in a dotted path must be non-null. Database providers such as Entity Framework may translate navigation access with their own null semantics.

## Apply dynamic ordering

```csharp
IOrderedQueryable<Order> ordered = query.OrderByDynamic("CreatedAt", descending: true);
ordered = ordered.ThenByDynamic("Id", descending: false);
```

`OrderByDynamic()` creates the primary ordering. Call `ThenByDynamic()` only on an already ordered query. Property types remain strongly typed in the generated expression; values are not converted to strings for sorting.

## Apply request options

```csharp
IQueryable<Order> filtered = query.AddRequestDataOptions(options);
```

`AddRequestDataOptions()` combines all exact filters, range filters, and search into one `Where` predicate, then applies order entries in list order. Exact filters and range filters are joined with `AND`; search fields are joined with `OR` and the complete search clause is joined to the filters with `AND`.

It does not apply `PageSize`, `ContinuationToken`, or `IncludeCount`; paging and counting remain the caller's responsibility. All methods return deferred queries and do not execute or materialize them.
