package subscription

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jensneuse/abstractlogger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jensneuse/graphql-go-tools/pkg/starwars"
)

type handlerRoutine func(ctx context.Context) func() bool

func TestHandler_Handle(t *testing.T) {
	starwars.SetRelativePathToStarWarsPackage("../starwars")

	t.Run("connection_init", func(t *testing.T) {
		_, client, handlerRoutine := setupSubscriptionHandlerTest(t)

		t.Run("should send connection error message when error on read occurrs", func(t *testing.T) {
			client.prepareConnectionInitMessage().withError().and().resetReceivedMessages()

			ctx, cancelFunc := context.WithCancel(context.Background())

			cancelFunc()
			require.Eventually(t, handlerRoutine(ctx), 1*time.Second, 5*time.Millisecond)

			expectedMessage := Message{
				Type:    MessageTypeConnectionError,
				Payload: jsonizePayload(t, "could not read message from client"),
			}

			messagesFromServer := client.readFromServer()
			assert.Contains(t, messagesFromServer, expectedMessage)
		})

		t.Run("should successfully init connection and respond with ack", func(t *testing.T) {
			client.prepareConnectionInitMessage().withoutError().and().resetReceivedMessages()

			ctx, cancelFunc := context.WithCancel(context.Background())

			cancelFunc()
			require.Eventually(t, handlerRoutine(ctx), 1*time.Second, 5*time.Millisecond)

			expectedMessage := Message{
				Type: MessageTypeConnectionAck,
			}

			messagesFromServer := client.readFromServer()
			assert.Contains(t, messagesFromServer, expectedMessage)
		})
	})

	t.Run("connection_keep_alive", func(t *testing.T) {
		subscriptionHandler, client, handlerRoutine := setupSubscriptionHandlerTest(t)

		t.Run("should successfully send keep alive messages after connection_init", func(t *testing.T) {
			keepAliveInterval, err := time.ParseDuration("5ms")
			require.NoError(t, err)

			subscriptionHandler.ChangeKeepAliveInterval(keepAliveInterval)

			client.prepareConnectionInitMessage().withoutError().and().resetReceivedMessages()
			ctx, cancelFunc := context.WithCancel(context.Background())

			handlerRoutineFunc := handlerRoutine(ctx)
			go handlerRoutineFunc()

			expectedMessage := Message{
				Type: MessageTypeConnectionKeepAlive,
			}

			messagesFromServer := client.readFromServer()
			waitForKeepAliveMessage := func() bool {
				for len(messagesFromServer) < 2 {
					messagesFromServer = client.readFromServer()
				}
				return true
			}

			assert.Eventually(t, waitForKeepAliveMessage, 1*time.Second, 5*time.Millisecond)
			assert.Contains(t, messagesFromServer, expectedMessage)

			cancelFunc()
		})
	})

	t.Run("subscription query", func(t *testing.T) {
		subscriptionHandler, client, handlerRoutine := setupSubscriptionHandlerTest(t)

		t.Run("should start subscription on start", func(t *testing.T) {
			payload := starwars.LoadQuery(t, starwars.FileRemainingJedisSubscription, nil)
			client.prepareStartMessage("1", payload).withoutError().and().resetReceivedMessages()

			ctx, cancelFunc := context.WithCancel(context.Background())
			handlerRoutineFunc := handlerRoutine(ctx)
			go handlerRoutineFunc()

			time.Sleep(10 * time.Millisecond)
			cancelFunc()

			expectedMessage := Message{
				Id:      "1",
				Type:    MessageTypeData,
				Payload: []byte(`{"data":null}`),
			}

			messagesFromServer := client.readFromServer()
			assert.Contains(t, messagesFromServer, expectedMessage)
			assert.Equal(t, 1, subscriptionHandler.ActiveSubscriptions())
		})

		t.Run("should stop subscription on stop", func(t *testing.T) {
			client.prepareStopMessage("1").withoutError().and().resetReceivedMessages()

			ctx, cancelFunc := context.WithCancel(context.Background())
			handlerRoutineFunc := handlerRoutine(ctx)
			go handlerRoutineFunc()

			waitForCanceledSubscription := func() bool {
				for subscriptionHandler.ActiveSubscriptions() > 0 {
				}
				return true
			}

			assert.Eventually(t, waitForCanceledSubscription, 1*time.Second, 5*time.Millisecond)
			assert.Equal(t, 0, subscriptionHandler.ActiveSubscriptions())

			cancelFunc()
		})
	})

	t.Run("connection_terminate", func(t *testing.T) {
		_, client, handlerRoutine := setupSubscriptionHandlerTest(t)

		t.Run("should successfully disconnect from client", func(t *testing.T) {
			client.prepareConnectionTerminateMessage().withoutError()
			require.True(t, client.connected)

			ctx, cancelFunc := context.WithCancel(context.Background())

			cancelFunc()
			require.Eventually(t, handlerRoutine(ctx), 1*time.Second, 5*time.Millisecond)

			assert.False(t, client.connected)
		})
	})

	t.Run("client is disconnected", func(t *testing.T) {
		_, client, handlerRoutine := setupSubscriptionHandlerTest(t)

		t.Run("server should not read from client and stop handler", func(t *testing.T) {
			err := client.Disconnect()
			require.NoError(t, err)
			require.False(t, client.connected)

			client.prepareConnectionInitMessage().withoutError()
			ctx, cancelFunc := context.WithCancel(context.Background())

			cancelFunc()
			require.Eventually(t, handlerRoutine(ctx), 1*time.Second, 5*time.Millisecond)

			assert.False(t, client.serverHasRead)
		})
	})

}

func setupSubscriptionHandlerTest(t *testing.T) (subscriptionHandler *Handler, client *mockClient, routine handlerRoutine) {
	client = newMockClient()

	var err error
	subscriptionHandler, err = NewHandler(abstractlogger.NoopLogger, client, starwars.NewExecutionHandler(t))
	require.NoError(t, err)

	routine = func(ctx context.Context) func() bool {
		return func() bool {
			subscriptionHandler.Handle(ctx)
			return true
		}
	}

	return subscriptionHandler, client, routine
}

func jsonizePayload(t *testing.T, payload interface{}) json.RawMessage {
	jsonBytes, err := json.Marshal(payload)
	require.NoError(t, err)

	return jsonBytes
}
