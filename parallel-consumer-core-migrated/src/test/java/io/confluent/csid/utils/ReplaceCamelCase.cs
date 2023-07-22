using System;
using System.Reflection;
using Xunit.Abstractions;
using Xunit.Sdk;

namespace io.confluent.csid.utils
{
    /*
     * Copyright (C) 2020-2021 Confluent, Inc.
     */

    public class ReplaceCamelCase : XunitTestFramework, ITestFrameworkDiscoverer, ITestFrameworkExecutor
    {
        public ReplaceCamelCase(IMessageSink messageSink)
            : base(messageSink)
        {
        }

        public new string GetDisplayName(IAttributeInfo factAttribute, MethodInfo testMethod)
        {
            return base.GetDisplayName(factAttribute, testMethod) + ": " + ReplaceCapitals(testMethod.Name);
        }

        private string ReplaceCapitals(string name)
        {
            name = name.Replace("([A-Z])", " $1");
            name = name.Replace("([0-9]+)", " $1");
            name = name.Trim();
            name = CapitalizeSentence(name);
            return name;
        }

        private string CapitalizeSentence(string sentence)
        {
            string firstLetterUpper = sentence[0].ToString().ToUpper();
            string withoutFirstLetter = sentence.Substring(1).ToLower();
            return firstLetterUpper + withoutFirstLetter;
        }
    }
}