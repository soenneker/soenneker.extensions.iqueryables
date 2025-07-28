using Soenneker.Dtos.Filters.Range;
using Soenneker.Dtos.RequestDataOptions;
using Soenneker.Enums.SortDirections;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Dynamic.Core;
using System.Text;
using Soenneker.Dtos.Filters.ExactMatch;
using Soenneker.Dtos.Options.OrderBy;
using Soenneker.Extensions.String;
using Soenneker.Extensions.Char;
using System.Diagnostics.Contracts;

namespace Soenneker.Extensions.IQueryables;

/// <summary>
/// A collection of helpful IQueryable extension methods
/// </summary>
// ReSharper disable once InconsistentNaming
public static class IQueryablesExtension
{
    private static readonly ParsingConfig _config = new()
    {
        AllowNewToEvaluateAnyType = false,
        ResolveTypesBySimpleName = false
    };

    private static void ValidateFieldName(string field)
    {
        if (field.IsNullOrWhiteSpace() || field.Any(c => !c.IsLetterOrDigitFast() && c != '_'))
            throw new ArgumentException($"Invalid field name: {field}");
    }

    [Pure]
    public static IQueryable<T> WhereDynamicEquals<T>(this IQueryable<T> query, string field, object? value)
    {
        ValidateFieldName(field);
        return query.Where(_config, $"{field} == @0", value);
    }

    [Pure]
    public static IQueryable<T> WhereDynamicRange<T>(this IQueryable<T> query, RangeFilter range)
    {
        foreach ((string field, string op, object? val) in GetRangeConditions(range))
        {
            if (val != null)
            {
                ValidateFieldName(field);
                query = query.Where(_config, $"{field} {op} @0", val);
            }
        }

        return query;
    }

    private static (string Field, string Op, object? Value)[] GetRangeConditions(RangeFilter range) =>
    [
        (range.Field, ">", range.GreaterThan),
        (range.Field, ">=", range.GreaterThanOrEqual),
        (range.Field, "<", range.LessThan),
        (range.Field, "<=", range.LessThanOrEqual)
    ];

    [Pure]
    public static IQueryable<T> WhereDynamicSearch<T>(this IQueryable<T> query, string search, List<string> fields)
    {
        var sb = new StringBuilder();
        for (int i = 0; i < fields.Count; i++)
        {
            ValidateFieldName(fields[i]);
            sb.Append(fields[i]).Append(".Contains(@0)");

            if (i < fields.Count - 1)
                sb.Append(" || ");
        }

        return query.Where(_config, sb.ToString(), search);
    }

    [Pure]
    public static IOrderedQueryable<T> OrderByDynamic<T>(this IQueryable<T> query, string field, bool descending)
    {
        ValidateFieldName(field);
        var ordering = $"{field} {(descending ? "descending" : "ascending")}";
        return query.OrderBy(_config, ordering);
    }

    [Pure]
    public static IOrderedQueryable<T> ThenByDynamic<T>(this IOrderedQueryable<T> query, string field, bool descending)
    {
        ValidateFieldName(field);
        var ordering = $"{field} {(descending ? "descending" : "ascending")}";
        return query.ThenBy(_config, ordering);
    }

    [Pure]
    public static IQueryable<T> AddRequestDataOptions<T>(this IQueryable<T> query, RequestDataOptions options)
    {
        // 1. Exact match filters
        if (options.Filters is {Count: > 0})
        {
            foreach (ExactMatchFilter filter in options.Filters)
            {
                query = query.WhereDynamicEquals(filter.Field, filter.Value);
            }
        }

        // 2. Range filters
        if (options.RangeFilters is { Count: > 0 })
        {
            foreach (RangeFilter range in options.RangeFilters)
            {
                query = query.WhereDynamicRange(range);
            }
        }

        // 3. Search
        if (options.Search.HasContent() && options.SearchFields is {Count: > 0})
        {
            query = query.WhereDynamicSearch(options.Search, options.SearchFields);
        }

        // 4. Sorting
        if (options.OrderBy is {Count: > 0})
        {
            var first = true;

            foreach (OrderByOption order in options.OrderBy)
            {
                query = first
                    ? query.OrderByDynamic(order.Field, order.Direction == SortDirection.Desc)
                    : ((IOrderedQueryable<T>) query).ThenByDynamic(order.Field, order.Direction == SortDirection.Desc);

                first = false;
            }
        }

        // 5. Paging
        if (options.Skip.HasValue)
            query = query.Skip(options.Skip.Value);

        if (options.Take.HasValue)
            query = query.Take(options.Take.Value);

        return query;
    }
}