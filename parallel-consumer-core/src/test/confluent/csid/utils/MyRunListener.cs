using System;
using NUnit.Engine;

namespace io.confluent.csid.utils
{
    /*-
     * Copyright (C) 2020-2022 Confluent, Inc.
     */

    [AutoService(typeof(TestExecutionListener))]
    public class MyRunListener : TestExecutionListener
    {
        private readonly string template = "\n" +
            "=========\n" +
            "   JUNIT {0}:    {1} ({2})\n" +
            "=========";

        public override void TestPlanExecutionStarted(ITestPlan testPlan)
        {
            Log(StringUtils.Msg(template, "Test plan execution started", testPlan, ""));
        }

        private void Log(string msg)
        {
            Console.WriteLine(msg);
        }

        public override void ExecutionSkipped(ITestIdentifier testIdentifier, string reason)
        {
            Log(StringUtils.Msg(template, "skipped", testIdentifier.DisplayName, reason));
        }

        public override void ExecutionStarted(ITestIdentifier testIdentifier)
        {
            Log(StringUtils.Msg(template, "started", testIdentifier.DisplayName, testIdentifier.LegacyReportingName));
        }
    }
}