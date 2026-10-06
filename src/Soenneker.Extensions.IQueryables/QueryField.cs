using System;
using System.Linq;
using System.Linq.Expressions;
namespace Soenneker.Extensions.IQueryables;
public sealed class QueryField<T, TKey> : IQueryField<T>
{
    private readonly Expression<Func<T, TKey>> _selector;
    public QueryField(Expression<Func<T, TKey>> selector) => _selector = selector ?? throw new ArgumentNullException(nameof(selector));
    public IOrderedQueryable<T> OrderBy(IQueryable<T> source, bool descending = false) => descending ? source.OrderByDescending(_selector) : source.OrderBy(_selector);
    public IOrderedQueryable<T> ThenBy(IOrderedQueryable<T> source, bool descending = false) => descending ? source.ThenByDescending(_selector) : source.ThenBy(_selector);
}
