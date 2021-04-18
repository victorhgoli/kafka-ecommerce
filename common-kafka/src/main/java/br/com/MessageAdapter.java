package br.com;

import com.google.gson.*;

import java.lang.reflect.Type;

public class MessageAdapter implements JsonSerializer<Message>, JsonDeserializer<Message> {
    @Override
    public JsonElement serialize(Message message, Type type, JsonSerializationContext context) {
        JsonObject json = new JsonObject();
        json.addProperty("type", message.getPayload().getClass().getName());
        json.add("payload", context.serialize(message.getPayload()));
        json.add("correlationId", context.serialize(message.getId()));
        return json;
    }

    @Override
    public Message deserialize(JsonElement json, Type type, JsonDeserializationContext context) throws JsonParseException {
       var obj = json.getAsJsonObject();
       var payloadType = obj.get("type").getAsString();
       var correlationId = (CorrelationId)  context.deserialize(obj.get("correlationId"), CorrelationId.class);

        try {
            var payload = context.deserialize(obj.get("payload"), Class.forName(payloadType));
            return new Message(correlationId, payload);
        } catch (ClassNotFoundException e) {
            throw new JsonParseException(e);
        }
    }
}
