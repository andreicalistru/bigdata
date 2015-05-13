<?php 
$logIp = "178.154.179.250";//"66.249.74.147";
$logPath	= "work1";
$startTime		= microtime(true);
$bigData = new bigDataBandWidth($logPath,$logIp);
$endTime = microtime(true);
$totalTime = $endTime - $startTime;

echo "total Time: $totalTime";//.date("H:i:s",strototime($totalTime));

class bigDataBandWidth
{
	public $logFiles	= array();
	public $logPath		= "";
	public $logIp		= "";
	public function __construct($logPath,$logIp)
	{
		$this->logPath	= $logPath;
		$this->logIp	= $logIp;
		$this->readDir();
		$this->parseLogs();
	}
	
	public function readDir()
	{
		if ($handle = opendir($this->logPath)) 
		{
			$logFiles = array();
			
			/* This is the correct way to loop over the directory. */
			while (false !== ($entry = readdir($handle)))
			{
				if (preg_match("(^.*.log)",$entry))
				{			
					$this->logFiles[] = $entry;
				}
			}
			closedir($handle);
		}
	}
	
	public function parseLogs()
	{
		$statistics = 0;		
		$statisticsList = array();
		$statisticsTotals = array();
		foreach ($this->logFiles as $logFile)
		{
			$fileStatistics = array();
			$buffer_size = 4096; // read 4kb at a time
			$file = gzopen($this->logPath."/".$logFile, 'rb'); 
			while(!gzeof($file)) {
				$read = gzread($file, $buffer_size);
				//if (preg_match("/".$this->logIp."\s-\s-\s\[.+]\s\".+HTTP\/1.1\"\s(200)\s(\d+)/", $read, $results))
				if (preg_match("/".$this->logIp."\s-\s-\s\[.+]\s\".+HTTP\/1.1\"\s(\d+)\s(\d+)/", $read, $results))
				{	
					if ($logFile === "Site0-access.log")
					{
						var_export($results);
					}
					//var_export($results);
					if (isset($fileStatistics[$results[1]])) $fileStatistics[$results[1]] += $results[2];
					else $fileStatistics[$results[1]] = $results[2];
					if (isset($statisticsTotals[$results[1]])) $statisticsTotals[$results[1]] += $results[2];
					else $statisticsTotals[$results[1]] = $results[2];
				}
			}
			/*foreach ($fileStatistics as $status => $stat)
			{
				if (isset($statisticsList[$status])) $statisticsList[$status] += $stat;
				else $statisticsList[$status] = $stat;
			}*/
			//$statistics += $fileStatistics;			
			$statisticsList[$logFile] = $fileStatistics;
			gzclose($file);
		}

		var_export($statisticsList);
		
		ksort($statisticsTotals);
		foreach ($statisticsTotals as $key => $stat)
		{
			$statistics += $stat;
			
			echo "code: $key: $stat\n";
			
		}
		echo "IP: ".$this->logIp." bandwidth: $statistics \n";
	}
}
?>