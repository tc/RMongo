library("RUnit")
library("RMongo")

testsuite.rmongo <- defineTestSuite("rmongo", dirs=file.path("."))

testResult <- runTestSuite(testsuite.rmongo)
printTextProtocol(testResult)
