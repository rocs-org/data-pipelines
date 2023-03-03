CREATE TABLE datenspende.epoch_value_types (
    type_id INT,
    type_name VARCHAR
);

INSERT INTO datenspende.epoch_value_types



id,code
100,Temperature
1000,Steps
1001,CoveredDistance
1002,FloorsClimbed
1003,ElevationGain
1004,ElevationLoss
1010,BurnedCalories
1011,ActiveBurnedCalories
1012,MetabolicEquivalent
1101,ActivityLowBinary
1102,ActivityMidBinary
1103,ActivityHighBinary
1104,ActivitySedentaryBinary
1111,DoffedBinary
1112,SleepBinary
1113,RestBinary
1114,ActiveBinary
1115,WalkBinary
1116,RunBinary
1117,BikeBinary
1118,TransportBinary
1200,ActivityType
1201,ActivityTypeDetail1
1202,ActivityTypeDetail2
1210,ActivityIntensity
1260,Cadence
1261,Speed
1265,PowerInWatts
1714,CoveredDistanceActive
1715,CoveredDistanceWalk
1716,CoveredDistanceRun
1717,CoveredDistanceBike
1726,CoveredDistanceRunManual
1727,CoveredDistanceBikeManual
2000,SleepStateBinary
2001,SleepInBedBinary
2002,SleepREMBinary
2003,SleepDeepBinary
2005,SleepLightBinary
2006,SleepAwakeBinary
2007,SleepLatencyBinary
2008,SleepAwakeAfterWakeUpBinary
2102,SleepInterruptionBinary
3000,HeartRate
3001,HeartRateResting
3002,HeartRateRestingHourly
3009,SPO2
3029,InterbeatIntervals
3100,Rmssd
4000,RespirationRate
4002,RespirationRateSleep
5020,Weight
5030,Height