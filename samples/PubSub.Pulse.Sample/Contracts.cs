using PubSub;

namespace PubSub.Pulse.Sample;

[PubSubTopic("orders.placed", Exchange = "shop.events")]
public sealed record OrderPlaced(Guid OrderId, decimal Total);

[PubSubTopic("payment.captured", Exchange = "shop.events")]
public sealed record PaymentCaptured(Guid OrderId, decimal Amount);

[PubSubTopic("inventory.reserved", Exchange = "shop.events")]
public sealed record InventoryReserved(Guid OrderId, string Sku, int Quantity);
