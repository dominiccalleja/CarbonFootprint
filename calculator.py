
""" 
Library provisional class structure 

1. BQConnector
   - subclass of Big Query processor

2. Consumer - characteristics of a client <cpu time, vm's, storage etc.>
    2.1 - Services - reading from 1. and computing for Consumer. 

3. Service - behaviour and characteristics of each possible service
    - ability to simulate temporal effects 
    - iherits characteristics from the zone data

4. Tools
    - AnalysisLib 
        - Optimisation
        - Schduling
        - CostEstimation  
    - LiveDemos
    - Hosting 

"""
