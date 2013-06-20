package kol

type Operation int

const (
	Create Operation = 1 << iota
	Update
	Remove
)

type Subscriber func(obj interface{})

func Subscribe(obj interface{}, ops Operation, subscriber Subscriber) {
}

func SubscriberQuery(obj interface{}, ops Operation, subscriber Subscriber) {
}
