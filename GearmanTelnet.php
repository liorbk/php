<?php
/**
 * Monitoring Gearman over telnet port 4730
 * 
 * So the only way to monitor Gearman is via doing a telnet to port 4730. The 
 * current monitoring supported commands are fairly basic. 
 * There are plans to include more set of commands in the next release.
 *
 * BigTent Design, Inc.
 *
 * @category   BigTent
 * @package    GearmanTelnet
 * @copyright  Copyright (c) 2009 BigTent Design, Inc. (http://www.bigtent.com)
 * @version    $Id: GearmanTelnet.php 16541 2009-10-12 06:59:03Z preilly $
 */
class GearmanTelnet {

	/**
	 * Default Values
	 */
	private $_gearmand = null;
	private $_gearmand_host = 'localhost';
	private $_gearmand_port = 4730;
	private $_timeout = 3;
	private $_errno = null;
	private $_errstr = '';
	private $_buffer = array();

	public function __construct($gearmand_host = null, $gearmand_port = null, $timeout = null) {

		$this->_gearmand_host = (!empty($gearmand_host)) ? $gearmand_host : $this->_gearmand_host;
		$this->_gearmand_port = (!empty($gearmand_port)) ? $gearmand_port : $this->_gearmand_port;
		$this->_timeout = (!empty($timeout)) ? $timeout : $this->_timeout;

		$this->_connect();
	}

	private function _connect() {
		$this->_gearmand = fsockopen($this->_gearmand_host, $this->_gearmand_port, $this->_errno, $this->_errstr, $this->_timeout);
	
		if(!$this->_gearmand) {
			throw new Exception("Failed to connect to gearmand_host at " . $this->_gearmand_host);
		}
	}

	/**
	 * Command: STATUS
	 *
	 * The output format of this function is tab separated columns as follows, 
	 * below are the columns shown:
	 *
	 * - Function name : A string denoting the name of the function of the job
	 * - Number in queue : A positive integer indicating the total number of 
	 * jobs for this function in the queue. This includes currently running ones 
	 * as well (next column)
	 * - Number of jobs running : A positive integer showing how many jobs of 
	 * this function are currently running
	 * - Number of capable workers : A positive integer denoting the maximum 
	 * possible count of workers that could be doing this job. Though they may 
	 * not all be working on it due to other tasks holding them busy.
	 *
	 */

	/* GEARMAN_COMMAND_GET_STATUS */
	public function getStatus() {
		if ($this->exec('STATUS')) {
			$get_status = $this->_getBuffer();
			echo implode("", $get_status);
		}
	}

	/**
	 * Command : Workers
	 *
	 * This command show the details of various clients registered with the 
	 * gearmand server. For each worker it shows the following info:
	 *
	 * - Peer IP: Client remote host
	 * - Client ID: Unique ID assigned to client
	 * - Functions: List of functions this client has registered for.
	 */

	/* GEARMAN_COMMAND_WORK_STATUS */
	public function getWorkers() {
		if ($this->exec('WORKERS')) {
			$get_status = $this->_getBuffer();
			echo implode("", $get_status);
		}
	}

	private function _getBuffer() {
		return $this->_buffer;
	}

	/** 
	 * The output format of this function is tab separated columns, 
	 * followed by a line consisting of a full stop and a newline (".\n") to 
	 * indicate the end of output.
	 *
	 * Any other command text throws a error "ERR unknown_command Unknown+server+command"
	 *
	 */
	private function exec($command) {
		$this->_buffer = array();
		if($this->_gearmand) {
			fputs ($this->_gearmand, $command . PHP_EOL);
			while (!feof($this->_gearmand)) {
				$l = fgets($this->_gearmand, 128);
				if ($l === '.' . PHP_EOL) {
					break;
				}
				else {
					$this->_buffer[] = $l;
				}
			}
			return true;
		}
		else {
			throw new Exception("Failed to connect to gearmand_host at " . $this->_gearmand_host);
		}
	}

	private function _disconnect() {
		fclose($this->_gearmand);
	}

 	/**
     * Destructor. Cleans up socket connection and command buffer
     * 
     * @return void 
     */
    public function __destruct() {
    	
    	// cleanup resources
    	$this->_disconnect();
    	$this->_buffer = array();
    }
}

/**
 * Tests
 */
//$gearman_telnet = new GearmanTelnet();
//$gearman_telnet->getStatus();
//$gearman_telnet->getWorkers();
