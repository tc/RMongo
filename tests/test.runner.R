library("RUnit")
testsuite.rmongo <- defineTestSuite("rmongo", dirs=file.path("."))

testResult <- runTestSuite(testsuite.rmongo)
printTextProtocol(testResult)
