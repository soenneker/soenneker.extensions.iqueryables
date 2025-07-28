using Soenneker.Dtos.Filters.ExactMatch;
using Soenneker.Dtos.Filters.Range;
using Soenneker.Dtos.Options.OrderBy;
using Soenneker.Dtos.RequestDataOptions;
using Soenneker.Enums.SortDirections;
using Soenneker.Extensions.String;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text.RegularExpressions;

namespace Soenneker.Extensions.IQueryables;

// ReSharper disable once UnusedType.Global
public static class IQueryablesExtension
{
    private static readonly Regex _fieldPattern = new(@"^[A-Za-z0-9_\.]+$", RegexOptions.Compiled);
    private static readonly ConcurrentDictionary<(Type, string), PropertyInfo[]> _propertyChainCache = new();

    private static readonly MethodInfo _orderBy =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.OrderBy) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _orderByDesc =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.OrderByDescending) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _thenBy =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.ThenBy) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _thenByDesc =
        typeof(Queryable).GetMethods().Single(m => m.Name == nameof(Queryable.ThenByDescending) && m.GetParameters().Length == 2);

    private static readonly MethodInfo _stringContains = typeof(string).GetMethod(nameof(string.Contains), [typeof(string)])!;

    public static IQueryable<T> WhereDynamicEquals<T>(this IQueryable<T> source, string field, object? value)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        UnaryExpression constant = Expression.Convert(Expression.Constant(value), member.Type);
        BinaryExpression body = Expression.Equal(member, constant);
        return source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    public static IQueryable<T> WhereDynamicRange<T>(this IQueryable<T> source, RangeFilter range)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, range.Field);
        Expression? body = null;

        void Add(object? v, Func<Expression, Expression, BinaryExpression> op)
        {
            if (v is null) return;
            UnaryExpression c = Expression.Convert(Expression.Constant(v), member.Type);
            body = body is null ? op(member, c) : Expression.AndAlso(body, op(member, c));
        }

        Add(range.GreaterThan, Expression.GreaterThan);
        Add(range.GreaterThanOrEqual, Expression.GreaterThanOrEqual);
        Add(range.LessThan, Expression.LessThan);
        Add(range.LessThanOrEqual, Expression.LessThanOrEqual);

        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    public static IQueryable<T> WhereDynamicSearch<T>(this IQueryable<T> source, string search, List<string> fields)
    {
        if (string.IsNullOrWhiteSpace(search) || fields.Count == 0)
            return source;

        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        Expression? body = null;

        foreach (string field in fields)
        {
            MemberExpression member = BuildMemberAccess<T>(param, field);
            if (member.Type != typeof(string)) continue;

            MethodCallExpression call = Expression.Call(member, _stringContains, Expression.Constant(search));
            body = body is null ? call : Expression.OrElse(body, call);
        }

        return body is null ? source : source.Where(Expression.Lambda<Func<T, bool>>(body, param));
    }

    public static IOrderedQueryable<T> OrderByDynamic<T>(this IQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);
        MethodInfo method = (descending ? _orderByDesc : _orderBy).MakeGenericMethod(typeof(T), member.Type);
        return (IOrderedQueryable<T>) method.Invoke(null, [source, lambda])!;
    }

    public static IOrderedQueryable<T> ThenByDynamic<T>(this IOrderedQueryable<T> source, string field, bool descending)
    {
        ParameterExpression param = Expression.Parameter(typeof(T), "x");
        MemberExpression member = BuildMemberAccess<T>(param, field);
        LambdaExpression lambda = Expression.Lambda(member, param);
        MethodInfo method = (descending ? _thenByDesc : _thenBy).MakeGenericMethod(typeof(T), member.Type);
        return (IOrderedQueryable<T>) method.Invoke(null, [source, lambda])!;
    }

    public static IQueryable<T> AddRequestDataOptions<T>(this IQueryable<T> query, RequestDataOptions opts)
    {
        if (opts.Filters?.Count > 0)
        {
            foreach (ExactMatchFilter f in opts.Filters)
            {
                query = query.WhereDynamicEquals(f.Field, f.Value);
            }
        }

        if (opts.RangeFilters?.Count > 0)
        {
            foreach (RangeFilter r in opts.RangeFilters)
            {
                query = query.WhereDynamicRange(r);
            }
        }

        if (opts.Search.HasContent() && opts.SearchFields?.Count > 0)
            query = query.WhereDynamicSearch(opts.Search, opts.SearchFields);

        if (opts.OrderBy?.Count > 0)
        {
            var first = true;

            foreach (OrderByOption o in opts.OrderBy)
            {
                query = first
                    ? query.OrderByDynamic(o.Field, o.Direction == SortDirection.Desc)
                    : ((IOrderedQueryable<T>) query).ThenByDynamic(o.Field, o.Direction == SortDirection.Desc);
                first = false;
            }
        }

        if (opts.Skip is > 0)
            query = query.Skip(opts.Skip.Value);

        if (opts.Take is > 0)
            query = query.Take(opts.Take.Value);

        return query;
    }

    private static MemberExpression BuildMemberAccess<T>(ParameterExpression root, string fieldPath)
    {
        ValidateFieldPath(fieldPath);

        PropertyInfo[] chain = _propertyChainCache.GetOrAdd((typeof(T), fieldPath), static key =>
        {
            (Type type, string path) = key;
            string[] segments = path.Split('.');
            var list = new List<PropertyInfo>(segments.Length);
            Type current = type;

            foreach (string segment in segments)
            {
                PropertyInfo prop = current.GetProperty(segment, BindingFlags.Instance | BindingFlags.Public | BindingFlags.IgnoreCase) ??
                                    throw new ArgumentException($"Field \"{path}\" does not exist on type {type.Name}");
                list.Add(prop);
                current = prop.PropertyType;
            }

            return list.ToArray();
        });

        Expression currentExpr = root;
        
        foreach (PropertyInfo prop in chain)
        {
            currentExpr = Expression.Property(currentExpr, prop);
        }
        return (MemberExpression) currentExpr;
    }

    private static void ValidateFieldPath(string fieldPath)
    {
        if (fieldPath.IsNullOrWhiteSpace() || !_fieldPattern.IsMatch(fieldPath))
            throw new ArgumentException($"Invalid field name: \"{fieldPath}\"");
    }
}