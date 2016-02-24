<?php
/**
 * Created by PhpStorm.
 * User: Admin
 * Date: 24.02.2016
 * Time: 12:28
 */

namespace app\commands;

use Faker\Provider\zh_TW\DateTime;
use PDOException;
use yii\console\Controller;
use yii\db\mssql\PDO;

class ParserController extends Controller
{
    //hardcoding database access data for speed - it's not a good practice
    protected
        $source = ['host' => 'devreplica.lan',
                   'usr' => 'testguitar',
                   'pwd' => 'zzVzz5317B',
                   'port' => '3303'
    ], $dest = ['host' => '192.168.14.243',
                'usr' => 'testguitar',
                'pwd' => 'zzVzz5317B',
                'port' => '3307'
    ], $limit = 100000,
        $bulk_size = 1000,
        $statistic = [],
        $numerals = ['first',
                     'second',
                     'third',
                     'fourth',
                     'fifth',
                     'sixth',
                     'seventh',
                     'eighth',
                     'ninth',
                     'tenth',
                     'eleventh',
                     'twelfth'];

    private $startTime;


    public function actionParse()
    {
        $this->start();

        $regexp = "/capo\\D{0,10}(\\d{1,2}|" . implode('|', $this->numerals) . ")/is";
        $no_capo_regexp = "/no +capo/is";

        $source = $this->getConnection($this->source);
        $destination = $this->getConnection($this->dest);

        $query = $source->prepare('SELECT content, id from content.tabs limit ' . $this->limit);
        $query->execute();

        $values = [];

        while ($result = $query->fetch(PDO::FETCH_ASSOC)) {
            $data = $result['content'];
            $id = $result['id'];

            //removing "no capo" entries
            $data = preg_replace($no_capo_regexp, '', $data);
            preg_match_all($regexp, $data, $matches);
            if (!empty($matches[0])) {
                $position = $matches[1][0]; //we deal only with first occurence
                if (!is_numeric($position)) {
                    $position = array_search($position, $this->numerals);
                }
                if ($position > 0) {
                    $values[] = [$id,
                                 $position];
                }
                if (count($values) == $this->bulk_size) {

                    $this->insert($destination, $values);
                    $values = [];
                }
            }
        }
        $this->insert($destination, $values);
        var_dump($this->stop());
    }

    public function actionGetStatistic()
    {
        $numeral = "/capo.{0,10}(first|second|third|fourth|fifth|sixth|seventh|eighth|ninth|tenth|eleventh|twelfth)/is";
        $digital = "/capo.{0,10}\\d{1,2}/is";
        $digital_matches = 0;
        $numeral_matches = 0;
        $other = 0;
        $total = 0;
        $capo_matches = [];

        $this->start();
        $connection = $this->getConnection($this->source);
        $file = fopen('statistic', 'w');
        if ($file === false) {
            var_dump('panic');
            die;
        }
        $query = $connection->prepare('SELECT content from content.tabs limit ' . $this->limit);
        $query->execute();
        while ($result = $query->fetch(PDO::FETCH_ASSOC)) {
            $data = $result['content'];
            $t = preg_match_all("/capo/i", $data, $tm);
            $total += $t;
            $data = preg_replace($numeral, '', $data, -1, $n_matches);
            $data = preg_replace($digital, '', $data, -1, $d_matches);
            $other += preg_match_all('/.{0,15}capo.{0,15}/i', $data, $matches);
            $digital_matches += $d_matches;
            $numeral_matches += $n_matches;
            if (is_array($matches[0])) {
                $capo_matches = array_merge($capo_matches, $matches[0]);
            }
        }

        fwrite($file, 'd:' . $digital_matches . "\r\n");
        fwrite($file, 'n:' . $numeral_matches . "\r\n");
        fwrite($file, 'o:' . $other . "\r\n");
        fwrite($file, 't:' . $total . "\r\n");
        fwrite($file, json_encode($capo_matches));
        var_dump($this->stop());
    }

    /**
     * @param PDO $connection
     * @param $data
     */
    protected function insert(PDO $connection, $data)
    {
        $query = 'INSERT INTO test.result (tab_id, capo_value) VALUES ';
        foreach ($data as $d) {
            $query .= '(' . $d[0] . ',' . $d[1] . '),';
        }
        $connection->exec(rtrim($query, ','));
        var_dump($connection->errorInfo());
    }

    /**
     * @return bool|PDO
     */
    protected function getConnection($cfg)
    {
        try {
            $host = $cfg['host'];
            $port = $cfg['port'];
            $connection = new PDO(
                "mysql:host=$host;port=$port;",
                $cfg['usr'],
                $cfg['pwd']
            );
            return $connection;
        } catch (\PDOException $err) {
            var_dump($err->getMessage());
            return false;
        }
    }

    protected function start()
    {
        $date = new \DateTime();
        $this->startTime = $date->getTimestamp();
    }

    protected function stop()
    {
        $date = new \DateTime();
        return $date->getTimestamp() - $this->startTime;
    }

}