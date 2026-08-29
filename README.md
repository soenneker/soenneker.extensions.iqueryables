[![](https://img.shields.io/nuget/v/soenneker.extensions.iqueryables.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.extensions.iqueryables/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.extensions.iqueryables/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.extensions.iqueryables/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.extensions.iqueryables.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.extensions.iqueryables/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.extensions.iqueryables/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.extensions.iqueryables/actions/workflows/codeql.yml)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.Extensions.IQueryables
A collection of helpful IQueryable extension methods.

## Installation

```bash
dotnet add package Soenneker.Extensions.IQueryables
```

## Quick start

```csharp
using Soenneker.Extensions.IQueryables;

// Given an existing IQueryable<T> named source:
var result = source.WhereDynamicEquals(field, value);
```

## Common operations

- `WhereDynamicEquals()` - Adds a deferred equality predicate for the property named by `field`; the provider translates it when the query executes.
- `WhereDynamicRange()` - Adds deferred lower and upper bounds from a `RangeFilter` without materializing the query.
- `WhereDynamicSearch()` - Adds a deferred search predicate across the requested property names.
- `OrderByDynamic()` - Creates the first ascending or descending ordering using a runtime property name.
- `ThenByDynamic()` - Adds a secondary ascending or descending ordering using a runtime property name.
- `AddRequestDataOptions()` - Applies filters, search, ordering, paging in one go.
