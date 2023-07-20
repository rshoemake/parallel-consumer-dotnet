using System;

namespace io.confluent.parallelconsumer.@internal
{
    /**
     * Single entry point for wrapping the actual execution of user functions
     */
    public static class UserFunctions
    {
        public static readonly string MSG = "Error occurred in code supplied by user";

        /**
         * @param <PARAM_ONE>      the first in type for the user function
         * @param <PARAM_TWO>      the second in type for the user function
         * @param <RESULT>         the out type for the user function
         * @param wrappedFunction  the function to run
         * @param userFuncParamOne a parameter to pass into the user's function
         * @param userFuncParamTwo a parameter to pass into the user's function
         */
        public static RESULT CarefullyRun<PARAM_ONE, PARAM_TWO, RESULT>(Func<PARAM_ONE, PARAM_TWO, RESULT> wrappedFunction,
                                                                        PARAM_ONE userFuncParamOne,
                                                                        PARAM_TWO userFuncParamTwo)
        {
            try
            {
                return wrappedFunction(userFuncParamOne, userFuncParamTwo);
            }
            catch (Exception e)
            {
                throw new ExceptionInUserFunctionException(MSG, e);
            }
        }

        /**
         * @param <PARAM>         the in type for the user function
         * @param <RESULT>        the out type for the user function
         * @param wrappedFunction the function to run
         * @param userFuncParam   the parameter to pass into the user's function
         */
        public static RESULT CarefullyRun<PARAM, RESULT>(Func<PARAM, RESULT> wrappedFunction, PARAM userFuncParam)
        {
            try
            {
                return wrappedFunction(userFuncParam);
            }
            catch (Exception e)
            {
                throw new ExceptionInUserFunctionException(MSG, e);
            }
        }

        /**
         * @param <PARAM>         the in type for the user function
         * @param wrappedFunction the function to run
         * @param userFuncParam   the parameter to pass into the user's function
         */
        public static void CarefullyRun<PARAM>(Action<PARAM> wrappedFunction, PARAM userFuncParam)
        {
            try
            {
                wrappedFunction(userFuncParam);
            }
            catch (Exception e)
            {
                throw new ExceptionInUserFunctionException(MSG, e);
            }
        }
    }
}