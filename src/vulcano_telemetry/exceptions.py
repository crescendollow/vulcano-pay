class VulcanoError(Exception):
   
    pass




class SQSConnectionError(VulcanoError):
   
    pass


class QueueTimeoutError(VulcanoError):
    
    pass


class CorruptedMessageError(VulcanoError):
    pass




class WebhookSignatureError(VulcanoError):
    
    pass


class WebhookTimestampError(VulcanoError):
    
    pass


class WebhookDeliveryError(VulcanoError):
    
    pass