from dslabmp import Context, Message, Process
import uuid

# AT MOST ONCE ---------------------------------------------------------------------------------------------------------

class AtMostOnceSender(Process):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id

    def on_local_message(self, msg: Message, ctx: Context):
        msg['uuid'] = uuid.uuid4().hex

        # deliver this info at most once
        ctx.send(msg, self._receiver)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        # process messages from receiver here
        pass

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


class AtMostOnceReceiver(Process):
    def __init__(self, node_id: str):
        self._id = node_id
        self.received_messages = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if not msg['uuid'] in self.received_messages:
            self.received_messages.append(msg['uuid'])
            msg._data.pop('uuid')
            ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# AT LEAST ONCE --------------------------------------------------------------------------------------------------------

class AtLeastOnceSender(Process):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id
        self.messages = {}

    def on_local_message(self, msg: Message, ctx: Context):
        msg['uuid'] = uuid.uuid4().hex
        # deliver this info at least once or exactly once
        ctx.send(msg, self._receiver)
        self.messages[msg['uuid']] = msg 
        ctx.set_timer(msg['uuid'], 5)
        

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg['uuid'] in self.messages:
            self.messages.pop(msg['uuid'])
            ctx.cancel_timer(msg['uuid'])

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name in self.messages:
            ctx.send(self.messages[timer_name], self._receiver)
            ctx.set_timer(timer_name, 5)


class AtLeastOnceReceiver(Process):
    def __init__(self, node_id: str):
        self._id = node_id

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        ctx.send(msg, sender)
        msg._data.pop('uuid')
        ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE ---------------------------------------------------------------------------------------------------------

class ExactlyOnceSender(Process):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id
        self.messages = {}

    def on_local_message(self, msg: Message, ctx: Context):
        msg['uuid'] = uuid.uuid4().hex
        # deliver this info at least once or exactly once
        ctx.send(msg, self._receiver)
        self.messages[msg['uuid']] = msg 
        ctx.set_timer(msg['uuid'], 5)
        

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if msg['uuid'] in self.messages:
            self.messages.pop(msg['uuid'])
            ctx.cancel_timer(msg['uuid'])

    def on_timer(self, timer_name: str, ctx: Context):
        if timer_name in self.messages:
            ctx.send(self.messages.get(timer_name), self._receiver)
            ctx.set_timer(timer_name, 5)


class ExactlyOnceReceiver(Process):
    def __init__(self, node_id: str):
        self._id = node_id
        self.received_messages = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        ctx.send(msg, sender)
        if not msg['uuid'] in self.received_messages:
            self.received_messages.append(msg['uuid'])
            msg._data.pop('uuid')
            ctx.send_local(msg)
            
    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass


# EXACTLY ONCE + ORDERED -----------------------------------------------------------------------------------------------

class ExactlyOnceOrderedSender(Process):
    def __init__(self, node_id: str, receiver_id: str):
        self._id = node_id
        self._receiver = receiver_id
        self.queue = []

    def on_local_message(self, msg: Message, ctx: Context):
        msg['uuid'] = uuid.uuid4().hex
        # deliver these info exactly once and keeping their order
        self.queue.append(msg)
        ctx.set_timer_once('queue', 0)

    def on_message(self, msg: Message, sender: str, ctx: Context):
        if len(self.queue) > 0 and msg['uuid'] == self.queue[0]['uuid']:
            self.queue.pop(0)
        if len(self.queue) == 0:
            ctx.cancel_timer('queue')

    def on_timer(self, timer_name: str, ctx: Context):
        if len(self.queue) > 0:
            ctx.send(self.queue[0], self._receiver)
            ctx.set_timer_once('queue', 10)
    


class ExactlyOnceOrderedReceiver(Process):
    def __init__(self, node_id: str):
        self._id = node_id
        self.received_messages = []

    def on_local_message(self, msg: Message, ctx: Context):
        # not used in this task
        pass

    def on_message(self, msg: Message, sender: str, ctx: Context):
        ctx.send(msg, sender)
        if not msg['uuid'] in self.received_messages:
            self.received_messages.append(msg['uuid'])
            msg._data.pop('uuid')
            ctx.send_local(msg)

    def on_timer(self, timer_name: str, ctx: Context):
        # process fired timers here
        pass
