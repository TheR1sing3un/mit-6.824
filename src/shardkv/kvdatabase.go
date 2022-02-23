package shardkv

type KvDataBase struct {
	KvData map[string]string //实际存放数据的map
}

func (kv *KvDataBase) Get(key string) (value string, ok bool) {
	if value, ok := kv.KvData[key]; ok {
		return value, ok
	}
	//若不存在
	return "", ok
}

func (kv *KvDataBase) Put(key string, value string) (newValue string) {
	kv.KvData[key] = value
	return value
}

func (kv *KvDataBase) Append(key string, arg string) (newValue string) {
	if value, ok := kv.KvData[key]; ok {
		//若存在,则直接append arg
		newValue := value + arg
		kv.KvData[key] = newValue
		return newValue
	}
	//若不存在,则直接put arg进去
	kv.KvData[key] = arg
	return arg
}

func (kv *KvDataBase) Merge(data map[string]string) {
	for key, value := range data {
		kv.KvData[key] = value
	}
}
