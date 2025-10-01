export async function GET() {
  return Response.json(
    { 
      status: 'ok', 
      timestamp: new Date().toISOString(),
      service: 'jobrunner-frontend'
    },
    { status: 200 }
  );
}