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
		foreach ($this->logFiles as $logFile)
		{
			$fileStatistics = 0;
			$buffer_size = 4096; // read 4kb at a time
			$file = gzopen($this->logPath."/".$logFile, 'rb'); 
			while(!gzeof($file)) {
				$read = gzread($file, $buffer_size);
				if (preg_match("/".$this->logIp."\s-\s-\s\[.+]\s\".+HTTP\/1.1\"\s(200)\s(\d+)/", $read, $results))
				{	
					$fileStatistics += $results[2]; 										
				}
			}
			$statistics += $fileStatistics;
			gzclose($file);
		}
		echo "IP: ".$this->logIp." bandwidth: $statistics \n";
	}
}
?>