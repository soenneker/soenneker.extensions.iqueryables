using System.Collections.Generic;
using System.Linq;
using Soenneker.Tests.Unit;

namespace Soenneker.Extensions.IQueryables.Tests;

// ReSharper disable once InconsistentNaming
public sealed class IQueryablesExtensionTests : UnitTest
{
    [Test]
    public async System.Threading.Tasks.Task Search_skips_null_string_values()
    {
        var values = new List<Item>
        {
            new() {Name = null},
            new() {Name = "matching value"}
        }.AsQueryable();

        List<Item> result = values.WhereDynamicSearch("matching", ["Name"]).ToList();

        await Assert.That(result.Count).IsEqualTo(1);
    }

    [Test]
    public async System.Threading.Tasks.Task Null_never_matches_a_non_nullable_property()
    {
        var values = new List<Item> {new() {Count = 1}}.AsQueryable();

        List<Item> result = values.WhereDynamicEquals("Count", null).ToList();

        await Assert.That(result.Count).IsEqualTo(0);
    }

    private sealed class Item
    {
        public string? Name { get; init; }
        public int Count { get; init; }
    }
}
