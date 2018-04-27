# SmartMoneyTracker

### Project Idea 
An open-source data pipeline to analyze and visualize abnormal events and notify users in real-time.

### What is the purpose, and most common use cases?
Options are one of the most versatile trading instruments, which offer a high-leverage approach to trading. For instance, you can construct a zero dollar option trade by selling calls and buying puts at the right strikes to effectively achieve [infinite leverage]. However, some strategies (e.g., selling unprotected options) could lead to unlimited risk. As a results, it is worth to follow the "smart money", which is controlled by institutional investors, market mavens and other financial professionals. 
 * `Use case`: Detect unusual options activity (e.g., abnormal volume) in realtime (for short term traders), and end-of-day review and analysis:


  identify the biggest change in open interest for equity options and new trades made by major players, which provide insights for swing trades.

### Which technologies are well-suited to solve those challenges? 
 1. Options transaction flow data handling requirement: high-throughput, low-latency platform for handling real-time data feeds. 
	* Proposed solution: Kafka + Flink

### What are the primary engineering challenges? Why would a Data Engineering Hiring Manager care about this project

### Proposed architecture
![alt text](image/flow_chart.png "Proposed architecture")

### What are the (quantitative) specifications/constraints for this project?
 1. Methods used to unusual options activity
	* Donoho, Steve. "Early detection of insider trading in option markets." Proceedings of the tenth ACM SIGKDD international conference on Knowledge discovery and data mining. ACM, 2004.


[infinite leverage]: <https://www.quora.com/How-much-leverage-can-I-expect-from-an-options-trade>
