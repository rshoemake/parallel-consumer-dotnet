using System;
using System.Collections.Generic;
using System.Linq;
using Xunit;

namespace io.confluent.csid.utils
{
    /*-
     * Copyright (C) 2020-2022 Confluent, Inc.
     */

    public class TrimListRepresentationTest
    {
        [Fact]
        public void CustomRepresentationFail()
        {
            List<int> one = Enumerable.Range(0, 1000).ToList();
            List<int> two = Enumerable.Range(999, 1001).ToList();
            Assert.Throws<Exception>(() => Assert.Contains(two, one, new TrimListRepresentation()));
        }

        [Fact]
        public void CustomRepresentationPass()
        {
            Assertions.UseRepresentation(new TrimListRepresentation());
            List<int> one = Enumerable.Range(0, 1000).ToList();
            List<int> two = Enumerable.Range(0, 1000).ToList();
            Assert.All(one, item => Assert.Contains(item, two));
        }
    }
}