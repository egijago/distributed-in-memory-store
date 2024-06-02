package entity

type Address struct {
	Host string
	Port string
}

func NewAddress(host string, port string) *Address {
	return &Address{
		Host: host,
		Port: port,
	}
}

func (a *Address) String() string {
	return a.Host + ":" + a.Port
}
