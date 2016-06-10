<?php
/**
 * database
 * User: L
 * Date: 2016/6/9
 * Time: 21:40
 */
define('DB_INSERT',1);
define('DB_REPLACE',2);
define('DB_STORE',3);

define('DB_BUCKET',262144);
define('DB_KEY_SIZE',128);
define('DB_INDEX_SIZE',DB_KEY_SIZE+12);

define('DB_KEY_EXISTS',1);
define('DB_FAILURE',-1);
define('DB_SUCCESS',0);
class database{
    private $idx_fp;
    private $dat_fp;
    private $is_open;
    public function open($path){
        $idx_file = $path.'/data_idx.idx';
        $dat_file = $path.'/data_dat.dat';
        if(file_exists($idx_file)){
            $init = false;
            $mode = 'r+b';
        }else{
            $init = true;
            $mode = 'w+b';
        }
        $this->idx_fp = fopen($idx_file,$mode);//索引文件
        $idx_ele = pack("L",'0x00000000');
        if($init){
            for ($i = 0;$i < DB_BUCKET; $i++){
                fwrite($this->idx_fp,$idx_ele,4);
            }
        }
        $this->dat_fp = fopen($dat_file,$mode);
        $this->is_open = true;

    }
    public function hash($key){
        $str = substr(md5($key),0,8);
        $hash = 0;
        for ($i = 0;$i<8;$i++){
            $hash .= $hash*33 + ord($str[$i]);
        }
        return $hash & 0x7fffffff;
    }
    public function fetch($key){
        if (strlen($key)>DB_KEY_SIZE){
            return DB_FAILURE;
        }
        $index = $this->hash($key)%DB_KEY_SIZE*4;

        fseek($this->idx_fp,$index,SEEK_SET);
        $pos = unpack("L",fread($this->idx_fp,4));
        $pos = $pos[1];


        $found = false;
        while ($pos){
            //not same go no
            fseek($this->idx_fp,$pos,SEEK_SET);
            $block = fread($this->idx_fp,DB_INDEX_SIZE);
            //get key
            $cpkey = substr($block,4,DB_KEY_SIZE);
            if(!strncmp($key,$cpkey,strlen($key))){
                //if key same
                $dataoff = unpack("L",substr($block,DB_KEY_SIZE+4,4));
                $dataoff = $dataoff[1];
                $datalen = unpack("L",substr($block,DB_KEY_SIZE+8,4));
                $datalen = $datalen[1];
                $found = true;
                break;
            }
            $pos = unpack("L",substr($block,0,4));
            $pos = $pos[1];
        }
        if(!$found){
            //can't found
            return NULL;
        }
        fseek($this->dat_fp,$dataoff,SEEK_SET);
        $data = fread($this->dat_fp,$datalen);
        return $data;
    }
    public function insert($key,$value){
        if (strlen($key)>DB_KEY_SIZE){
            return DB_FAILURE;
        }

        $idxoff = fstat($this->idx_fp);
        $dataoff = fstat($this->dat_fp);

        $idxoff = intval($idxoff['size']);
        $dataoff = intval($dataoff['size']);

        $keylen = strlen($key);

        //generation block start
        $block = pack("L",0x00000000);
        $block .= $key;
        $space =DB_KEY_SIZE - $keylen;
        for ($i = 0;$i<$space;$i++){
            $block .= pack("C",0x00);
        }
        $block .= pack("L",$dataoff);
        $block .= pack("L",strlen($value));
        //generation block end

        $index = $this->hash($key)%DB_KEY_SIZE*4;

        fseek($this->idx_fp,$index,SEEK_SET);
        $pos = unpack("L",fread($this->idx_fp,4));
        $pos = $pos[1];

        //如果该位置尚未存在数据
        if($pos==0){
            //插入数据
            fseek($this->idx_fp,$index,SEEK_SET);
            fwrite($this->idx_fp,pack("L",$idxoff),4);

            fseek($this->idx_fp,0,SEEK_END);
            fwrite($this->idx_fp,$block,DB_INDEX_SIZE);


            fseek($this->dat_fp,0,SEEK_END);
            fwrite($this->dat_fp,$value,strlen($value));
            return DB_SUCCESS;
        }
        $found = false;
        $prev = 0;
        while($pos){
            fseek($this->idx_fp,$pos,SEEK_SET);
            $tmp_block = fread($this->idx_fp,DB_INDEX_SIZE);
            $cpkey = substr($tmp_block,4,DB_KEY_SIZE);

            if(!strncmp($key,$cpkey,strlen($key))){
                $dataoff = unpack("L",substr($tmp_block,DB_KEY_SIZE+4,4));
                $dataoff = $dataoff[1];
                $datalen = unpack("L",substr($tmp_block,DB_KEY_SIZE+8,4));
                $datalen = $datalen[1];
                $found = true;
                break;
            }
            $prev = $pos;
            $pos = unpack("L",substr($tmp_block,0,4));
            $pos = $pos[1];
        }

        if($found){
            return DB_KEY_EXISTS;
        }

        //如果不存在插入数据
        fseek($this->idx_fp,$prev,SEEK_SET);
        fwrite($this->idx_fp,pack("L",$idxoff),4);

        fseek($this->idx_fp,0,SEEK_END);
        fwrite($this->idx_fp,$block,DB_INDEX_SIZE);

        fseek($this->dat_fp,0,SEEK_END);
        fwrite($this->dat_fp,$value,strlen($value));
        return DB_SUCCESS;
    }
    public function delete($key){
        if (strlen($key)>DB_KEY_SIZE){
            return DB_FAILURE;
        }
        $index = $this->hash($key)%DB_KEY_SIZE*4;
        fseek($this->idx_fp,$index);
        $pos = unpack("L",fread($this->idx_fp,4));
        $pos = $pos[1];
        if($pos==0){
            return DB_FAILURE;
        }
        $next = 0;
        $prev = 0;
        $found = false;
        while($pos){
            fseek($this->idx_fp,$pos,SEEK_SET);
            $block = fread($this->idx_fp,DB_INDEX_SIZE);
            $cpkey = unpack("L",substr($block,4,DB_KEY_SIZE));
            $next = unpack("L",substr($block,0,4));
            $next = $next[1];
            if(!strncmp($key,$cpkey,strlen($key))){
                $found = true;
                break;
                //if find the key
            }
            $prev = $pos;
            $pos = $next;
        }
        if(!$found){
            //没找到
            return DB_FAILURE;
        }
        if($prev){
            fseek($this->idx_fp,$prev,SEEK_SET);
            fwrite($this->idx_fp,unpack("L",$next),4);
        }else{
            fseek($this->idx_fp,$index);
            fwrite($this->idx_fp,unpack("L",$next),4);
        }
        return DB_SUCCESS;
    }
    public function close(){
        if($this->is_open){
            fclose($this->idx_fp);
            fclose($this->dat_fp);
        }
    }
}
$db = new database();
$db->open('.');
$start_time = explode(" ",microtime());
$start_time = $start_time[0] + $start_time[1];
for ($i=0;$i<1000;$i++){
    $arr = ["num"=>$i,'name'=>'name'.$i];
    $arr = json_encode($arr);
    echo $db->insert("key".$i,$arr)."\r\n";
}

$end_time = explode(" ",microtime());
$end_time = $end_time[0] + $end_time[1];
$db->close();
echo "process  time in".($end_time-$start_time).'seconds';